namespace EventFlow.AzureStorage.Config
{
	public class AzureStorageConfiguration : IAzureStorageConfiguration
	{
		public string StorageAccountConnectionString { get; set; }
		
		public string SystemContainerName { get; set; } = "eventflow-system-params";
		public int SequenceNumberRangeSize { get; set; } = 1000;
		public int SequenceNumberOptimisticConcurrencyRetries { get; set; } = 25;
		
//TODO: Use the official regex to validate these names (also for the system container).
//TODO: Add XML-docs describing allowed names (also for the system container).
		public string EventStoreTableName { get; set; } = "EventFlowEvents";
		public string ReadStoreTableName { get; set; } = "EventFlowReadModels";
		public string SnapshotStoreTableName { get; set; } = "EventFlowSnapshots";
	}
}