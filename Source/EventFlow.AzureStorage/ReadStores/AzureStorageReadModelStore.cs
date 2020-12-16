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

				var chunks = resultSegment.Results
					.Select((x, index) => new { Index = index, Value = x })
					.Where(x => x.Value != null)
					.GroupBy(x => x.Index / TableConstants.TableServiceBatchMaximumOperations)
					.Select(x => x.Select(v => v.Value));

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

//TODO: Consider using EntityResolver to store TReadModel directly. https://docs.microsoft.com/en-us/archive/blogs/windowsazurestorage/windows-azure-storage-client-library-2-0-tables-deep-dive#nosql
		public async Task<ReadModelEnvelope<TReadModel>> GetAsync(string id, CancellationToken cancellationToken)
		{
			var (partitionKey, rowKey) = GetKeys(id);
			var operation = TableOperation.Retrieve<ReadModelEntity>(partitionKey, rowKey);
			var table = _azureStorageFactory.CreateTableReferenceForReadStore();
			var result = await table.ExecuteAsync(operation, cancellationToken).ConfigureAwait(false);

			if (!(result.Result is ReadModelEntity entity))
			{
				_log.Verbose(() => $"Could not find any Azure Storage read model '{partitionKey}' with ID '{id}'");
				return ReadModelEnvelope<TReadModel>.Empty(id);
			}

			if (string.IsNullOrWhiteSpace(entity.Data))
			{
				_log.Verbose(() => $"Found Azure Storage read model '{partitionKey}' with ID '{id}', but without any data");
				return ReadModelEnvelope<TReadModel>.Empty(id);
			}

			var readModelEnvelope = CreateEnvelopeFromEntity(entity);
			_log.Verbose(() => $"Found Azure Storage read model '{partitionKey}' with ID '{id}' and version '{readModelEnvelope.Version}'");

			return readModelEnvelope;
		}

		public async Task UpdateAsync(
			IReadOnlyCollection<ReadModelUpdate> readModelUpdates,
			IReadModelContextFactory readModelContextFactory,
			Func<IReadModelContext, IReadOnlyCollection<IDomainEvent>, ReadModelEnvelope<TReadModel>, CancellationToken, Task<ReadModelUpdateResult<TReadModel>>> updateReadModel,
			CancellationToken cancellationToken)
		{
			IEnumerable<ReadModelEnvelope<TReadModel>> readModelEnvelopes;
			if (readModelUpdates.Count == 1)
				readModelEnvelopes = await RetrieveSingleEnvelopeAsync(readModelUpdates.Single().ReadModelId, cancellationToken).ConfigureAwait(false);
			else
				readModelEnvelopes = RetrieveMultipleEnvelopes(readModelUpdates.Select(rmu => rmu.ReadModelId));

			// When retrieving from Azure Storage Tables, missing entities are omitted.
			// Perform a left join to produce one context for each incoming update.
			var contexts = readModelUpdates
				.LeftJoinAsync(
					readModelEnvelopes,
					u => u.ReadModelId,
					e => e.ReadModelId,
					async u =>
						{
							var readModelEnvelope = await CreateEnvelopeForMissingModelAsync(u.ReadModelId, cancellationToken).ConfigureAwait(false);
							var readModelContext = readModelContextFactory.Create(u.ReadModelId, true);
							return new ReadModelUpdateContext(u, readModelEnvelope, readModelContext);
						},
					(u, e) =>
						{
							var readModelContext = readModelContextFactory.Create(u.ReadModelId, false);
							return Task.FromResult(new ReadModelUpdateContext(u, e, readModelContext));
						},
					null);

			await foreach (var context in contexts.WithCancellation(cancellationToken).ConfigureAwait(false))
			{
				var originalVersion = context.ReadModelEnvelope.Version;

				var readModelUpdateResult = await updateReadModel(
					context.ReadModelContext,
					context.ReadModelUpdate.DomainEvents,
					context.ReadModelEnvelope,
					cancellationToken).ConfigureAwait(false);
				if (!readModelUpdateResult.IsModified)
					continue;

				if (context.ReadModelContext.IsMarkedForDeletion)
				{
					await DeleteAsync(context.ReadModelId, cancellationToken).ConfigureAwait(false);
					continue;
				}

				var readModelEnvelope = readModelUpdateResult.Envelope;
				
//TODO: SetVersion?
				
//TODO: Insert or Update.
			}
		}
		
		private async Task<IEnumerable<ReadModelEnvelope<TReadModel>>> RetrieveSingleEnvelopeAsync(string readModelId, CancellationToken cancellationToken)
		{
			var readModelEnvelope = await GetAsync(readModelId, cancellationToken).ConfigureAwait(false);
			return readModelEnvelope.AsEnumerable();
		}

		// Retrieving multiple distinct records from an Azure Storage Table is no trivial task.
		// Since each retrieval is a network request, there is a lot of over-head associated with it.
		// For better performance, group as many as possible into a single request.
		internal IEnumerable<ReadModelEnvelope<TReadModel>> RetrieveMultipleEnvelopes(IEnumerable<string> readModelIds)
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
				var envelopes = results.Select(CreateEnvelopeFromEntity);

				foreach (var envelope in envelopes)
					yield return envelope;
			}
		}

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

		private static ReadModelEnvelope<TReadModel> CreateEnvelopeFromEntity(ReadModelEntity entity)
		{
			var readModel = JsonConvert.DeserializeObject<TReadModel>(entity.Data);
			var readModelVersion = entity.Version;
			var readModelEnvelope = ReadModelEnvelope<TReadModel>.With(entity.RowKey, readModel, readModelVersion);
			return readModelEnvelope;
		}

		private async Task<ReadModelEnvelope<TReadModel>> CreateEnvelopeForMissingModelAsync(string readModelId, CancellationToken cancellationToken)
		{
			var readModel = await _readModelFactory.CreateAsync(readModelId, cancellationToken).ConfigureAwait(false);
			var readModelEnvelope = ReadModelEnvelope<TReadModel>.With(readModelId, readModel);
			return readModelEnvelope;
		}


		private class ReadModelUpdateContext
		{
			public ReadModelUpdateContext(
				ReadModelUpdate readModelUpdate,
				ReadModelEnvelope<TReadModel> readModelEnvelope,
				IReadModelContext readModelContext)
			{
				ReadModelUpdate = readModelUpdate;
				ReadModelEnvelope = readModelEnvelope;
				ReadModelContext = readModelContext;
			}

			public string ReadModelId => ReadModelUpdate.ReadModelId;
			public ReadModelUpdate ReadModelUpdate { get; }
			public ReadModelEnvelope<TReadModel> ReadModelEnvelope { get; }
			public IReadModelContext ReadModelContext { get; }
		}


		private class ReadModelEntity : TableEntity
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