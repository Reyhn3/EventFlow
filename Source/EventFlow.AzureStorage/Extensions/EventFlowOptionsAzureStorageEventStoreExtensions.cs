using EventFlow.AzureStorage.EventStores;
using EventFlow.Extensions;


namespace EventFlow.AzureStorage.Extensions
{
	public static class EventFlowOptionsAzureStorageEventStoreExtensions
	{
//TODO: If not necessary to be separate, combine with UseAzureStorage-extension.
		public static IEventFlowOptions UseAzureStorageEventStore(this IEventFlowOptions eventFlowOptions)
			=> eventFlowOptions.UseEventStore<AzureStoragePersistence>();
	}
}