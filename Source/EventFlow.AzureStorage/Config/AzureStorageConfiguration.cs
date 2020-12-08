namespace EventFlow.AzureStorage.Config
{
	public class AzureStorageConfiguration : IAzureStorageConfiguration
	{
		public string StorageAccountConnectionString { get; set; }
		
		public string SystemContainerName { get; set; } = "eventflow-system-params";
		public int SequenceNumberRangeSize { get; set; } = 1000;
		public int SequenceNumberOptimisticConcurrencyRetries { get; set; } = 25;
		
		public string EventStoreTableName { get; set; } = "EventFlowEvents";
		public string ReadStoreTableName { get; set; } = "EventFlowReadModels";
	}
}