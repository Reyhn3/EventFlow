using EventFlow.AzureStorage.SnapshotStores;
using EventFlow.Extensions;


namespace EventFlow.AzureStorage.Config
{
	public static class EventFlowOptionsAzureStorageSnapshotStoreExtensions
	{
		public static IEventFlowOptions UseAzureStorageSnapshotStore(this IEventFlowOptions eventFlowOptions)
			=> eventFlowOptions.UseSnapshotStore<AzureStorageSnapshotPersistence>();
	}
}