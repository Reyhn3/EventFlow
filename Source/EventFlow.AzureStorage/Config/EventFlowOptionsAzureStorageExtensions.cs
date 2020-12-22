using EventFlow.AzureStorage.Connection;
using EventFlow.AzureStorage.EventStores;
using EventFlow.Configuration;


namespace EventFlow.AzureStorage.Config
{
	public static class EventFlowOptionsAzureStorageExtensions
	{
		public static IEventFlowOptions UseAzureStorage(this IEventFlowOptions eventFlowOptions)
		{
			return eventFlowOptions
				.RegisterServices(sr =>
					{
						sr.Register<IBootstrap, AzureStorageBootstrap>();
						sr.Register<IUniqueIdGenerator, UniqueIdGenerator>(Lifetime.Singleton);
						sr.Register<IOptimisticSyncStore, BlobOptimisticSyncStore>(Lifetime.Singleton);
					});
		}

		public static IEventFlowOptions ConfigureAzureStorage(this IEventFlowOptions eventFlowOptions, IAzureStorageConfiguration azureStorageConfiguration)
		{
			return eventFlowOptions
				.RegisterServices(f =>
					{
						f.Register<IAzureStorageFactory, AzureStorageFactory>();
						f.Register(_ => azureStorageConfiguration, Lifetime.Singleton);
					});
		}
	}
}