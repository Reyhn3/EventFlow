using EventFlow.ReadStores;


namespace EventFlow.AzureStorage.ReadStores
{
	public interface IAzureStorageReadModelStore<TReadModel> : IReadModelStore<TReadModel>
		where TReadModel : class, IReadModel
	{}
}