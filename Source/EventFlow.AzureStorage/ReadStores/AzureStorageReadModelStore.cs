using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.Aggregates;
using EventFlow.AzureStorage.Connection;
using EventFlow.Extensions;
using EventFlow.Logs;
using EventFlow.ReadStores;
using Microsoft.Azure.Cosmos.Table;
using Newtonsoft.Json;


namespace EventFlow.AzureStorage.ReadStores
{
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

		public Task DeleteAsync(string id, CancellationToken cancellationToken)
			=> throw new NotImplementedException();

		public Task DeleteAllAsync(CancellationToken cancellationToken)
			=> throw new NotImplementedException();

		public async Task<ReadModelEnvelope<TReadModel>> GetAsync(string id, CancellationToken cancellationToken)
		{
			var partitionKey = typeof(TReadModel).PrettyPrint();
			var rowKey = id;
			var operation = TableOperation.Retrieve<ReadModelEntity>(partitionKey, rowKey);
			var table = _factory.CreateTableReferenceForReadStore();
			var result = await table.ExecuteAsync(operation, CancellationToken.None).ConfigureAwait(false);

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
			=> throw new NotImplementedException();


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