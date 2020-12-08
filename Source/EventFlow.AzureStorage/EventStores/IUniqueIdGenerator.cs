using System.Threading.Tasks;


namespace EventFlow.AzureStorage.EventStores
{
	public interface IUniqueIdGenerator
	{
		Task<long> GetNextIdAsync();
	}
}