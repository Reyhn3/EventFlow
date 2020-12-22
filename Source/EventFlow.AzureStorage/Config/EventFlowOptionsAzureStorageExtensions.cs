using System;
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

		public static IEventFlowOptions ConfigureAzureStorage(this IEventFlowOptions eventFlowOptions, string storageAccountConnectionString)
			=> ConfigureAzureStorage(eventFlowOptions, new AzureStorageConfiguration {StorageAccountConnectionString = storageAccountConnectionString});

		public static IEventFlowOptions ConfigureAzureStorage(this IEventFlowOptions eventFlowOptions, AzureStorageConfiguration azureStorageConfiguration)
			=> ConfigureAzureStorage(eventFlowOptions, azureStorageConfiguration, null);

		public static IEventFlowOptions ConfigureAzureStorage(this IEventFlowOptions eventFlowOptions, Action<AzureStorageConfiguration> config)
			=> ConfigureAzureStorage(eventFlowOptions, null, config);

		public static IEventFlowOptions ConfigureAzureStorage(this IEventFlowOptions eventFlowOptions, AzureStorageConfiguration azureStorageConfiguration, Action<AzureStorageConfiguration> config)
		{
			azureStorageConfiguration ??= new AzureStorageConfiguration();
			config ??= c => {};
			config(azureStorageConfiguration);

			return eventFlowOptions
				.RegisterServices(f =>
					{
						f.Register<IAzureStorageFactory, AzureStorageFactory>();
						f.Register(_ => azureStorageConfiguration, Lifetime.Singleton);
					});
		}
	}
}