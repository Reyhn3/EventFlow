using System;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.AzureStorage.IntegrationTests.ReadStores.ReadModels;
using EventFlow.Queries;
using EventFlow.ReadStores;
using EventFlow.TestHelpers.Aggregates.Queries;


namespace EventFlow.AzureStorage.IntegrationTests.ReadStores.QueryHandlers
{
	public class AzureStorageThingyGetVersionQueryHandler : IQueryHandler<ThingyGetVersionQuery, long?>
	{
		private readonly IReadModelStore<AzureStorageThingyReadModel> _readStore;

		public AzureStorageThingyGetVersionQueryHandler(IReadModelStore<AzureStorageThingyReadModel> readStore)
		{
			_readStore = readStore ?? throw new ArgumentNullException(nameof(readStore));
		}

		public async Task<long?> ExecuteQueryAsync(ThingyGetVersionQuery query, CancellationToken cancellationToken)
		{
			var readModel = await _readStore.GetAsync(query.ThingyId.Value, cancellationToken).ConfigureAwait(false);
			return readModel?.Version;
		}
	}
}