using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;


namespace EventFlow.AzureStorage.Connection
{
	public interface IAzureStorageFactory
	{
		Task InitializeAsync();
		CloudTable CreateTableReferenceForEventStore();
	}
}