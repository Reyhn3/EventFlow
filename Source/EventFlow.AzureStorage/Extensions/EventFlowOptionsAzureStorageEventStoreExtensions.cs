using EventFlow.AzureStorage.EventStores;
using EventFlow.Extensions;


namespace EventFlow.AzureStorage.Extensions
{
	public static class EventFlowOptionsAzureStorageEventStoreExtensions
	{
		public static IEventFlowOptions UseAzureStorageEventStore(this IEventFlowOptions eventFlowOptions)
			=> eventFlowOptions.UseEventStore<AzureStoragePersistence>();
	}
}