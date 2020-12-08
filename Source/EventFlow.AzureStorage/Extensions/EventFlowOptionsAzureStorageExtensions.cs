using EventFlow.AzureStorage.Config;
using EventFlow.AzureStorage.Connection;
using EventFlow.Configuration;


namespace EventFlow.AzureStorage.Extensions
{
	public static class EventFlowOptionsAzureStorageExtensions
	{
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