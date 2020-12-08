namespace EventFlow.AzureStorage.Config
{
	public class AzureStorageConfiguration : IAzureStorageConfiguration
	{
		public string StorageAccountConnectionString { get; set; }
		public string EventStoreTableName { get; set; } = "EventFlowEvents";
	}
}