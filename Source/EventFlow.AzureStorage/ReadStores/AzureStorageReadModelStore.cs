using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.Aggregates;
using EventFlow.AzureStorage.Connection;
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
		private readonly IAzureStorageFactory _factory;
		private readonly ILog _log;

		public AzureStorageReadModelStore(ILog log, IAzureStorageFactory factory)
		{
			_log = log ?? throw new ArgumentNullException(nameof(log));
			_factory = factory ?? throw new ArgumentNullException(nameof(factory));
		}

		public async Task DeleteAsync(string id, CancellationToken cancellationToken)
		{
			var (partitionKey, rowKey) = GetKeys(id);
			var retrieveOperation = TableOperation.Retrieve<ReadModelEntity>(partitionKey, rowKey);
			var table = _factory.CreateTableReferenceForReadStore();
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
			var table = _factory.CreateTableReferenceForReadStore();

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

		public async Task<ReadModelEnvelope<TReadModel>> GetAsync(string id, CancellationToken cancellationToken)
		{
			var (partitionKey, rowKey) = GetKeys(id);
			var operation = TableOperation.Retrieve<ReadModelEntity>(partitionKey, rowKey);
			var table = _factory.CreateTableReferenceForReadStore();
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

			var readModel = JsonConvert.DeserializeObject<TReadModel>(entity.Data);
			var readModelVersion = entity.Version;

			_log.Verbose(() => $"Found Azure Storage read model '{partitionKey}' with ID '{id}' and version '{readModelVersion}'");

			return ReadModelEnvelope<TReadModel>.With(id, readModel, readModelVersion);
		}

		public Task UpdateAsync(
			IReadOnlyCollection<ReadModelUpdate> readModelUpdates,
			IReadModelContextFactory readModelContextFactory,
			Func<IReadModelContext, IReadOnlyCollection<IDomainEvent>, ReadModelEnvelope<TReadModel>, CancellationToken, Task<ReadModelUpdateResult<TReadModel>>> updateReadModel,
			CancellationToken cancellationToken)
		{
			throw new NotImplementedException();
		}

		private static string GetPartitionKey()
			=> typeof(TReadModel).PrettyPrint();

		private static (string partitionKey, string rowKey) GetKeys(string id)
			=> (GetPartitionKey(), id);

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