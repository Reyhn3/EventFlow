using System;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.AzureStorage.Connection;
using EventFlow.Configuration;


namespace EventFlow.AzureStorage.Config
{
	public class AzureStorageBootstrap : IBootstrap
	{
		private readonly IAzureStorageFactory _azureStorageFactory;

		public AzureStorageBootstrap(IAzureStorageFactory azureStorageFactory)
		{
			_azureStorageFactory = azureStorageFactory ?? throw new ArgumentNullException(nameof(azureStorageFactory));
		}

		public async Task BootAsync(CancellationToken cancellationToken)
		{
			await _azureStorageFactory.InitializeAsync().ConfigureAwait(false);
		}
	}
}