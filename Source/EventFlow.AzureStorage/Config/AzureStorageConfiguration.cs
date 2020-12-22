using System;


namespace EventFlow.AzureStorage.Config
{
	public class AzureStorageConfiguration
	{
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

		public void Validate()
		{
			if (string.IsNullOrWhiteSpace(StorageAccountConnectionString))
				throw new ArgumentNullException(nameof(StorageAccountConnectionString), "The storage account connection string must be set");

			if (string.IsNullOrWhiteSpace(nameof(SystemContainerName)))
				throw new ArgumentNullException(nameof(SystemContainerName));

			if (SequenceNumberRangeSize < 1)
				throw new ArgumentOutOfRangeException(nameof(SequenceNumberRangeSize), SequenceNumberRangeSize,
					$"{nameof(SequenceNumberRangeSize)} must be 1 or greater");

			if (SequenceNumberOptimisticConcurrencyRetries < 1)
				throw new ArgumentOutOfRangeException(nameof(SequenceNumberOptimisticConcurrencyRetries), SequenceNumberOptimisticConcurrencyRetries,
					$"{nameof(SequenceNumberOptimisticConcurrencyRetries)} must be 1 or greater");

			if (string.IsNullOrWhiteSpace(nameof(EventStoreTableName)))
				throw new ArgumentNullException(nameof(EventStoreTableName));

			if (string.IsNullOrWhiteSpace(nameof(ReadStoreTableName)))
				throw new ArgumentNullException(nameof(ReadStoreTableName));

			if (string.IsNullOrWhiteSpace(nameof(SnapshotStoreTableName)))
				throw new ArgumentNullException(nameof(SnapshotStoreTableName));
		}
	}
}