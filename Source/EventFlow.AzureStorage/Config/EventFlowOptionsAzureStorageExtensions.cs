using System;
using EventFlow.AzureStorage.Connection;
using EventFlow.AzureStorage.EventStores;
using EventFlow.Configuration;


namespace EventFlow.AzureStorage.Config
{
	public static class EventFlowOptionsAzureStorageExtensions
	{
		public static IEventFlowOptions UseAzureStorage(
			this IEventFlowOptions eventFlowOptions,
			string storageAccountConnectionString)
			=> UseAzureStorage(eventFlowOptions, new AzureStorageConfiguration {StorageAccountConnectionString = storageAccountConnectionString});

		public static IEventFlowOptions UseAzureStorage(
			this IEventFlowOptions eventFlowOptions,
			AzureStorageConfiguration azureStorageConfiguration)
			=> UseAzureStorage(eventFlowOptions, azureStorageConfiguration, null);

		public static IEventFlowOptions UseAzureStorage(
			this IEventFlowOptions eventFlowOptions,
			Action<AzureStorageConfiguration> config)
			=> UseAzureStorage(eventFlowOptions, null, config);

		private static IEventFlowOptions UseAzureStorage(
			this IEventFlowOptions eventFlowOptions,
			AzureStorageConfiguration azureStorageConfiguration,
			Action<AzureStorageConfiguration> config)
		{
			azureStorageConfiguration ??= new AzureStorageConfiguration();
			config ??= c => {};
			config(azureStorageConfiguration);

			azureStorageConfiguration.Validate();

			return eventFlowOptions
				.RegisterServices(sr =>
					{
						sr.Register(_ => azureStorageConfiguration, Lifetime.Singleton);
						sr.Register<IAzureStorageFactory, AzureStorageFactory>();
						sr.Register<IBootstrap, AzureStorageBootstrap>();
						sr.Register<IUniqueIdGenerator, UniqueIdGenerator>(Lifetime.Singleton);
						sr.Register<IOptimisticSyncStore, BlobOptimisticSyncStore>(Lifetime.Singleton);
					});
		}
	}
}