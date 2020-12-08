using System;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.AzureStorage.Connection;
using EventFlow.AzureStorage.EventStores;
using EventFlow.Configuration;


namespace EventFlow.AzureStorage.Config
{
	public class AzureStorageBootstrap : IBootstrap
	{
		private readonly IAzureStorageFactory _azureStorageFactory;
		private readonly IOptimisticSyncStore _optimisticSyncStore;

		public AzureStorageBootstrap(IAzureStorageFactory azureStorageFactory, IOptimisticSyncStore optimisticSyncStore)
		{
			_azureStorageFactory = azureStorageFactory ?? throw new ArgumentNullException(nameof(azureStorageFactory));
			_optimisticSyncStore = optimisticSyncStore ?? throw new ArgumentNullException(nameof(optimisticSyncStore));
		}

		public async Task BootAsync(CancellationToken cancellationToken)
		{
			await _azureStorageFactory.InitializeAsync().ConfigureAwait(false);
			await _optimisticSyncStore.InitializeAsync().ConfigureAwait(false);
		}
	}
}