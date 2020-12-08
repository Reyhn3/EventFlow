namespace EventFlow.AzureStorage.Config
{
	public interface IAzureStorageConfiguration
	{
		string StorageAccountConnectionString { get; }
		string EventStoreTableName { get; }
	}
}