namespace EventFlow.AzureStorage.Config
{
	public interface IAzureStorageConfiguration
	{
		string StorageAccountConnectionString { get; }

		string SystemContainerName { get; }
		int SequenceNumberRangeSize { get; }
		int SequenceNumberOptimisticConcurrencyRetries { get; }
		
		string EventStoreTableName { get; }
		string ReadStoreTableName { get; }
		string SnapshotStoreTableName { get; }
	}
}