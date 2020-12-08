using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.Azure.Cosmos.Table;


namespace EventFlow.AzureStorage.Connection
{
	public interface IAzureStorageFactory
	{
		Task InitializeAsync();
		CloudTable CreateTableReferenceForEventStore();
		CloudTable CreateTableReferenceForReadStore();
		BlobClient CreateBlobClientForSequenceNumber();
	}
}