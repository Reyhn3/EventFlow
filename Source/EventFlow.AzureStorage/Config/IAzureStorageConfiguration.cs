namespace EventFlow.AzureStorage.Config
{
	public interface IAzureStorageConfiguration
	{
		string StorageAccountConnectionString { get; }
		string SystemContainerName { get; }
		string EventStoreTableName { get; }
		int SequenceNumberRangeSize { get; }
		int SequenceNumberOptimisticConcurrencyRetries { get; }
	}
}