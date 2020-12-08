using System.Threading.Tasks;


namespace EventFlow.AzureStorage.EventStores
{
	public interface IOptimisticSyncStore
	{
		Task InitializeAsync();
		Task<long> GetCurrentAsync();
		Task<bool> TryOptimisticWriteAsync(long data);
	}
}