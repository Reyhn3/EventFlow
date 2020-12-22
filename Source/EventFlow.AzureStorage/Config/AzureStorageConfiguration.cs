namespace EventFlow.AzureStorage.Config
{
	public class AzureStorageConfiguration
	{
//TODO: Ensure this is never null.
		public virtual string StorageAccountConnectionString { get; set; }
		
//TODO: Add documentation
		public virtual string SystemContainerName { get; set; } = "eventflow-system-params";
		public virtual int SequenceNumberRangeSize { get; set; } = 1000;
		public virtual int SequenceNumberOptimisticConcurrencyRetries { get; set; } = 25;
		
//TODO: Use the official regex to validate these names (also for the system container).
//TODO: Add XML-docs describing allowed names (also for the system container).
		public virtual string EventStoreTableName { get; set; } = "EventFlowEvents";
		public virtual string ReadStoreTableName { get; set; } = "EventFlowReadModels";
		public virtual string SnapshotStoreTableName { get; set; } = "EventFlowSnapshots";
	}
}