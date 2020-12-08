using EventFlow.AzureStorage.ReadStores;
using EventFlow.Configuration;
using EventFlow.Extensions;
using EventFlow.ReadStores;


namespace EventFlow.AzureStorage.Extensions
{
	public static class EventFlowOptionsAzureStorageReadStoreExtensions
	{
		public static IEventFlowOptions UseAzureStorageReadModel<TReadModel, TReadModelLocator>(this IEventFlowOptions eventFlowOptions)
			where TReadModel : class, IReadModel
			where TReadModelLocator : IReadModelLocator
			=> eventFlowOptions
				.RegisterServices(RegisterAzureStorageReadStore<TReadModel>)
				.UseReadStoreFor<IAzureStorageReadModelStore<TReadModel>, TReadModel, TReadModelLocator>();

		public static IEventFlowOptions UseAzureStorageReadModel<TReadModel>(this IEventFlowOptions eventFlowOptions)
			where TReadModel : class, IReadModel
			=> eventFlowOptions
				.RegisterServices(RegisterAzureStorageReadStore<TReadModel>)
				.UseReadStoreFor<IAzureStorageReadModelStore<TReadModel>, TReadModel>();

		private static void RegisterAzureStorageReadStore<TReadModel>(IServiceRegistration serviceRegistration)
			where TReadModel : class, IReadModel
		{
			serviceRegistration.Register<IAzureStorageReadModelStore<TReadModel>, AzureStorageReadModelStore<TReadModel>>();
			serviceRegistration.Register<IReadModelStore<TReadModel>>(r => r.Resolver.Resolve<IAzureStorageReadModelStore<TReadModel>>());
		}
	}
}