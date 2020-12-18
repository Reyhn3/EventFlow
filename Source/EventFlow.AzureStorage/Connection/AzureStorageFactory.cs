using System;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
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

				var eventStore = CreateTableReferenceForEventStore();
				if (!eventStore.Exists())
					await eventStore.CreateIfNotExistsAsync().ConfigureAwait(false);

				var readStore = CreateTableReferenceForReadStore();
				if (!readStore.Exists())
					await readStore.CreateIfNotExistsAsync().ConfigureAwait(false);

				var snapshotStore = CreateTableReferenceForSnapshotStore();
				if (!snapshotStore.Exists())
					await snapshotStore.CreateIfNotExistsAsync().ConfigureAwait(false);

				var container = CreateBlobContainerClient();
				if (!await container.ExistsAsync().ConfigureAwait(false))
					await container.CreateIfNotExistsAsync().ConfigureAwait(false);

				_isInitialized = true;
			}
			finally
			{
				_semaphore.Release();
			}
		}

		public CloudTable CreateTableReferenceForEventStore()
			=> CreateTableReference(_configuration.EventStoreTableName);

		public CloudTable CreateTableReferenceForReadStore()
			=> CreateTableReference(_configuration.ReadStoreTableName);

		public CloudTable CreateTableReferenceForSnapshotStore()
			=> CreateTableReference(_configuration.SnapshotStoreTableName);

		private CloudTable CreateTableReference(string tableName)
		{
			if (string.IsNullOrWhiteSpace(tableName))
				throw new ArgumentNullException(nameof(tableName));

			var client = _cloudStorageAccount.CreateCloudTableClient();
			var table = client.GetTableReference(tableName);
			return table;
		}

		public BlobClient CreateBlobClientForSequenceNumber()
		{
			var container = CreateBlobContainerClient();
			var client = container.GetBlobClient(GlobalSequenceNumberBlobName);
			return client;
		}

		private BlobContainerClient CreateBlobContainerClient()
			=> new BlobContainerClient(_configuration.StorageAccountConnectionString, _configuration.SystemContainerName);
	}
}