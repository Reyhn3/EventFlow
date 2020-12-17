using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.Aggregates;
using EventFlow.AzureStorage.Connection;
using EventFlow.AzureStorage.Extensions;
using EventFlow.Extensions;
using EventFlow.Logs;
using EventFlow.ReadStores;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.Cosmos.Table.Protocol;
using Newtonsoft.Json;


namespace EventFlow.AzureStorage.ReadStores
{
	/*
	 * The read model store is using a single Azure Storage Table to store
	 * ALL read models. The read model type is used as partition key.
	 */
	public class AzureStorageReadModelStore<TReadModel> : IAzureStorageReadModelStore<TReadModel>
		where TReadModel : class, IReadModel
	{
		private static readonly string ReadModelName = typeof(TReadModel).PrettyPrint();
		private static readonly JsonSerializerSettings JsonSettings = new JsonSerializerSettings
			{
				ContractResolver = new PrivateContractResolver()
			};

		private readonly IAzureStorageFactory _azureStorageFactory;
		private readonly IReadModelFactory<TReadModel> _readModelFactory;
		private readonly ILog _log;

		public AzureStorageReadModelStore(ILog log, IAzureStorageFactory azureStorageFactory, IReadModelFactory<TReadModel> readModelFactory)
		{
			_log = log ?? throw new ArgumentNullException(nameof(log));
			_azureStorageFactory = azureStorageFactory ?? throw new ArgumentNullException(nameof(azureStorageFactory));
			_readModelFactory = readModelFactory ?? throw new ArgumentNullException(nameof(readModelFactory));
		}

		public async Task DeleteAsync(string id, CancellationToken cancellationToken)
		{
			var (partitionKey, rowKey) = GetKeys(id);
			var retrieveOperation = TableOperation.Retrieve<ReadModelEntity>(partitionKey, rowKey);
			var table = _azureStorageFactory.CreateTableReferenceForReadStore();
			var result = await table.ExecuteAsync(retrieveOperation, cancellationToken).ConfigureAwait(false);
			if (!(result.Result is ReadModelEntity entity))
				return;

			var deleteOperation = TableOperation.Delete(entity);
			await table.ExecuteAsync(deleteOperation, cancellationToken).ConfigureAwait(false);
		}

		public async Task DeleteAllAsync(CancellationToken cancellationToken)
		{
			var partitionKey = GetPartitionKey();
			var filter = TableQuery.GenerateFilterCondition(TableConstants.PartitionKey, QueryComparisons.Equal, partitionKey);
			var query = new TableQuery().Where(filter).Select(new[] {TableConstants.PartitionKey, TableConstants.RowKey});
			var table = _azureStorageFactory.CreateTableReferenceForReadStore();

			TableContinuationToken token = null;
			do
			{
				var resultSegment = await table.ExecuteQuerySegmentedAsync(query, token, cancellationToken).ConfigureAwait(false);
				token = resultSegment.ContinuationToken;

				var chunks = resultSegment.Results.Batch(TableConstants.TableServiceBatchMaximumOperations, true);
				foreach (var chunk in chunks)
				{
					var operation = new TableBatchOperation();
					foreach (var entity in chunk)
						operation.Delete(entity);

					await table.ExecuteBatchAsync(operation, cancellationToken).ConfigureAwait(false);
					_log.Verbose(() => $"Deleted batch of {operation.Count} Azure Storage read models '{partitionKey}'");
				}
			} while (token != null);
		}

		public async Task<ReadModelEnvelope<TReadModel>> GetAsync(string id, CancellationToken cancellationToken)
		{
			var entity = await RetrieveSingleEntityAsync(id, cancellationToken).ConfigureAwait(false);
			if (entity == null)
				return ReadModelEnvelope<TReadModel>.Empty(id);

			var readModelEnvelope = await CreateEnvelopeAsync(entity, cancellationToken).ConfigureAwait(false);
			_log.Verbose(() => $"Found Azure Storage read model '{GetPartitionKey()}' with ID '{id}' and version '{readModelEnvelope.Version}'");

			return readModelEnvelope;
		}

		public async Task UpdateAsync(
			IReadOnlyCollection<ReadModelUpdate> readModelUpdates,
			IReadModelContextFactory readModelContextFactory,
			Func<IReadModelContext, IReadOnlyCollection<IDomainEvent>, ReadModelEnvelope<TReadModel>, CancellationToken, Task<ReadModelUpdateResult<TReadModel>>> updateReadModel,
			CancellationToken cancellationToken)
		{
			IEnumerable<ReadModelEntity> readModelEntities;
			if (readModelUpdates.Count == 1)
				readModelEntities = (await RetrieveSingleEntityAsync(readModelUpdates.Single().ReadModelId, cancellationToken).ConfigureAwait(false)).AsEnumerable();
			else
				readModelEntities = RetrieveMultipleEntities(readModelUpdates.Select(rmu => rmu.ReadModelId));
			
			var contexts = readModelUpdates
				.LeftJoinAsync(
					readModelEntities,
					u => u.ReadModelId,
					e => e.RowKey,
					async u =>
						{
							var readModelEnvelope = await CreateEnvelopeAsync(u.ReadModelId, cancellationToken).ConfigureAwait(false);
							var readModelContext = readModelContextFactory.Create(u.ReadModelId, true);
							return new ReadModelUpdateContext(u, null, readModelEnvelope, readModelContext);
						},
					async (u, e) =>
						{
							var readModelEnvelope = await CreateEnvelopeAsync(e, cancellationToken).ConfigureAwait(false);
							var readModelContext = readModelContextFactory.Create(u.ReadModelId, false);
							return new ReadModelUpdateContext(u, e, readModelEnvelope, readModelContext);
						},
					null);

			var updatedContexts = contexts
				.ApplyOrExcludeAsync(async c =>
					{
						// This action returns a new ReadModelEnvelope that references
						// the same TReadModel, which has now been updated.
						var readModelUpdateResult = await updateReadModel(
							c.ReadModelContext,
							c.ReadModelUpdate.DomainEvents,
							c.ReadModelEnvelope,
							cancellationToken).ConfigureAwait(false);

						if (readModelUpdateResult.IsModified)
							c.ReadModelEnvelope = readModelUpdateResult.Envelope;
						
						return readModelUpdateResult.IsModified;
					}, cancellationToken);
				
			await PersistReadModelsAsync(updatedContexts, cancellationToken).ConfigureAwait(false);
		}

		private async Task PersistReadModelsAsync(IAsyncEnumerable<ReadModelUpdateContext> contexts, CancellationToken cancellationToken)
		{
			var table = _azureStorageFactory.CreateTableReferenceForReadStore();
			var batches = contexts.Batch(TableConstants.TableServiceBatchMaximumOperations);
			await foreach (var batch in batches.WithCancellation(cancellationToken).ConfigureAwait(false))
			{
				var operation = new TableBatchOperation();
				await batch.ForEachAsync(c => operation.Add(CreateEntityOperation(c)), cancellationToken).ConfigureAwait(false);
				await table.ExecuteBatchAsync(operation, cancellationToken).ConfigureAwait(false);
			}

			static TableOperation CreateEntityOperation(ReadModelUpdateContext context)
			{
				var (partitionKey, rowKey) = GetKeys(context.ReadModelId);
				
				if (context.ReadModelContext.IsMarkedForDeletion)
					return TableOperation.Delete(new ReadModelEntity(partitionKey, rowKey));

				var data = SerializeReadModel(context.ReadModelEnvelope.ReadModel);
				var entity = context.ReadModelEntity ?? new ReadModelEntity(partitionKey, rowKey);
				entity.Data = data;
				entity.Version = context.ReadModelEnvelope.Version ?? 0;
				entity.ReadModelType = ReadModelName;
				return TableOperation.InsertOrReplace(entity);
			}
		}

//TODO: Consider using EntityResolver to store TReadModel directly. https://docs.microsoft.com/en-us/archive/blogs/windowsazurestorage/windows-azure-storage-client-library-2-0-tables-deep-dive#nosql
		private async Task<ReadModelEntity> RetrieveSingleEntityAsync(string readModelId, CancellationToken cancellationToken)
		{
			var (partitionKey, rowKey) = GetKeys(readModelId);
			var operation = TableOperation.Retrieve<ReadModelEntity>(partitionKey, rowKey);
			var table = _azureStorageFactory.CreateTableReferenceForReadStore();
			var result = await table.ExecuteAsync(operation, cancellationToken).ConfigureAwait(false);
			
			if (!(result.Result is ReadModelEntity entity))
			{
				_log.Verbose(() => $"Could not find any Azure Storage read model '{partitionKey}' with ID '{rowKey}'");
				return null;
			}

			if (string.IsNullOrWhiteSpace(entity.Data))
			{
				_log.Verbose(() => $"Found Azure Storage read model '{partitionKey}' with ID '{rowKey}', but without any data");
				return null;
			}

			return entity;
		}
		
//TODO: Consider using EntityResolver to store TReadModel directly. https://docs.microsoft.com/en-us/archive/blogs/windowsazurestorage/windows-azure-storage-client-library-2-0-tables-deep-dive#nosql
		// Retrieving multiple distinct records from an Azure Storage Table is no trivial task.
		// Since each retrieval is a network request, there is a lot of over-head associated with it.
		// For better performance, group as many as possible into a single request.
		private IEnumerable<ReadModelEntity> RetrieveMultipleEntities(IEnumerable<string> readModelIds)
		{
			var partitionKey = GetPartitionKey();
			var partitionKeyFilter = TableQuery.GenerateFilterCondition(TableConstants.PartitionKey, QueryComparisons.Equal, partitionKey);

			// Estimate how many of the records can be retrieved in one query by calculating
			// the size of the HTTP query string.
			// Note that this does NOT account for URL-encoding of the filter string,
			// but the recursion depth is usually more limiting than the query string length.
			// (unless the PK/RK-values are very short).
			// Nor does it account for the head of the query string.
			var requestGroups = GroupByRunningLength(readModelIds);

			foreach (var requestGroup in requestGroups)
			{
				// This will create a nested OR-filter.
				// The C#-library does not allow specifying more than one condition,
				// nor does it allow writing the filter manually.
				// Otherwise a more efficient query would be e.g.
				// "(PartitionKey eq '<value>') and ((RowKey eq '<value>') or (RowKey eq '<value>') or (RowKey eq '<value>') or ...)".
				var rowKeyFilters = requestGroup
					.Aggregate((string)null, (filterSeed, rowKey) =>
						{
							var rowKeyFilter = TableQuery.GenerateFilterCondition(TableConstants.RowKey, QueryComparisons.Equal, rowKey);
							return filterSeed == null ? rowKeyFilter : TableQuery.CombineFilters(filterSeed, TableOperators.Or, rowKeyFilter);
						});
				var filter = TableQuery.CombineFilters(partitionKeyFilter, TableOperators.And, rowKeyFilters);
				var query = new TableQuery<ReadModelEntity>().Where(filter);
				var table = _azureStorageFactory.CreateTableReferenceForReadStore();
				var results = table.ExecuteQuery(query);

				foreach (var result in results)
					yield return result;
			}
		}

//TODO: Consider moving the run-length grouping into a separate class.
		private IEnumerable<IGrouping<int, string>> GroupByRunningLength(IEnumerable<string> readModelIds)
			=> GroupByRunningLength(readModelIds, CalculateQueryFilterMaxLength, CalculateRecordFilterLength);

		// This signature exists only to facilitate testing.
		internal IEnumerable<IGrouping<int, string>> GroupByRunningLength(IEnumerable<string> readModelIds, Func<int> calculateQueryFilterMaxLength, Func<string, int> calculateRecordFilterLength)
		{
			// This is a limitation that will show itself as an exception
			// with the error message "Recursion depth exceeded allowed limit"
			// if there are too many conditions in the filter string.
			// The value 100 has been found by trial-and-error.
			const int assumedMaxRecursionDepth = 100;

			var maxLength = calculateQueryFilterMaxLength();
			var remainingLength = maxLength;
			var remainingElements = assumedMaxRecursionDepth;
			var currentGroup = 0;

			return readModelIds
				.GroupBy(id =>
					{
						if (remainingElements-- == 0)
						{
							remainingElements = assumedMaxRecursionDepth - 1;
							remainingLength = maxLength;
							return ++currentGroup;
						}

						var length = calculateRecordFilterLength(id);
						if (length > maxLength)
						{
							if (remainingLength == maxLength)
								return currentGroup++;

							remainingLength = 0;
							return ++currentGroup;
						}

						if ((remainingLength -= length) >= 0)
							return currentGroup;

						remainingLength = maxLength - length;
						return ++currentGroup;
					});
		}

		private static int CalculateQueryFilterMaxLength()
		{
			// This is an assumed limitation based on the MSDN documentation
			// for the maximum total length of a query URL.
			const int assumedMaximumQueryStringSize = 4096;

			// This is the number of characters reserved for the
			// "(PartitionKey eq '') and ()" part of the filter.
			const int partitionKeyConditionLength = 27;

			return assumedMaximumQueryStringSize - partitionKeyConditionLength - GetPartitionKey().Length;
		}

		private static int CalculateRecordFilterLength(string readModelId)
		{
			// This is the number of characters reserved for the
			// "( or (RowKey eq ''))" part of the filter, including
			// the surrounding parentheses in the subquery nesting.
			const int rowKeyConditionLength = 20;

			return rowKeyConditionLength + readModelId.Length;
		}

		private static (string partitionKey, string rowKey) GetKeys(string id)
			=> (GetPartitionKey(), id);

		private static string GetPartitionKey()
			=> ReadModelName;
		
		// ReSharper disable once UnusedParameter.Local
		private static Task<ReadModelEnvelope<TReadModel>> CreateEnvelopeAsync(ReadModelEntity entity, CancellationToken cancellationToken)
		{
			var readModel = JsonConvert.DeserializeObject<TReadModel>(entity.Data, JsonSettings);
			var readModelVersion = entity.Version;
			var readModelEnvelope = ReadModelEnvelope<TReadModel>.With(entity.RowKey, readModel, readModelVersion);
			return Task.FromResult(readModelEnvelope);
		}

		private async Task<ReadModelEnvelope<TReadModel>> CreateEnvelopeAsync(string readModelId, CancellationToken cancellationToken)
		{
			var readModel = await _readModelFactory.CreateAsync(readModelId, cancellationToken).ConfigureAwait(false);
			var readModelEnvelope = ReadModelEnvelope<TReadModel>.With(readModelId, readModel);
			return readModelEnvelope;
		}

		private static string SerializeReadModel(TReadModel readModel)
		{
			var json = JsonConvert.SerializeObject(readModel, Formatting.Indented);
			return json;
		}


		private class ReadModelUpdateContext
		{
			public ReadModelUpdateContext(
				ReadModelUpdate readModelUpdate,
				ReadModelEntity readModelEntity,
				ReadModelEnvelope<TReadModel> readModelEnvelope,
				IReadModelContext readModelContext)
			{
				ReadModelUpdate = readModelUpdate;
				ReadModelEntity = readModelEntity;
				ReadModelEnvelope = readModelEnvelope;
				ReadModelContext = readModelContext;
			}

			public string ReadModelId => ReadModelUpdate.ReadModelId;
			public ReadModelUpdate ReadModelUpdate { get; }
			public ReadModelEntity ReadModelEntity { get; }
			public ReadModelEnvelope<TReadModel> ReadModelEnvelope { get; set; }
			public IReadModelContext ReadModelContext { get; }
		}


		internal class ReadModelEntity : TableEntity
		{
			public ReadModelEntity()
			{}

			public ReadModelEntity(string partitionKey, string rowKey)
				: base(partitionKey, rowKey)
			{}

			public string ReadModelType { get; set; }
			public long Version { get; set; }
			public string Data { get; set; }
		}
	}
}