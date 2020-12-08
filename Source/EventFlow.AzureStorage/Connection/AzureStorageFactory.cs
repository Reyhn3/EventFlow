using System;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.AzureStorage.Config;
using Microsoft.Azure.Cosmos.Table;


namespace EventFlow.AzureStorage.Connection
{
	public class AzureStorageFactory : IAzureStorageFactory
	{
		private const string GlobalSequenceNumberBlobName = "GlobalSequenceNumber";
		
		private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1);
		private bool _isInitialized;

		private readonly CloudStorageAccount _cloudStorageAccount;
		private readonly IAzureStorageConfiguration _configuration;

		public AzureStorageFactory(IAzureStorageConfiguration configuration)
		{
			_configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
			_cloudStorageAccount = CloudStorageAccount.Parse(configuration.StorageAccountConnectionString);
		}

		public async Task InitializeAsync()
		{
			await _semaphore.WaitAsync().ConfigureAwait(false);
			try
			{
				if (_isInitialized)
					return;

				var table = CreateTableReferenceForEventStore();
				await table.CreateIfNotExistsAsync().ConfigureAwait(false);

				_isInitialized = true;
			}
			finally
			{
				_semaphore.Release();
			}
		}

		public CloudTable CreateTableReferenceForEventStore()
		{
			if (string.IsNullOrWhiteSpace(_configuration.EventStoreTableName))
				throw new ArgumentNullException(nameof(_configuration.EventStoreTableName));

			var client = _cloudStorageAccount.CreateCloudTableClient();
			var table = client.GetTableReference(_configuration.EventStoreTableName);
			return table;
		}
	}
}