using EventFlow.AzureStorage.EventStores;
using EventFlow.Extensions;


namespace EventFlow.AzureStorage.Config
{
	public static class EventFlowOptionsAzureStorageEventStoreExtensions
	{
		public static IEventFlowOptions UseAzureStorageEventStore(this IEventFlowOptions eventFlowOptions)
			=> eventFlowOptions.UseEventStore<AzureStoragePersistence>();
	}
}