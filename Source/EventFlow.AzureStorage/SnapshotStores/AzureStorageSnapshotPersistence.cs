using System;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.AzureStorage.Connection;
using EventFlow.Core;
using EventFlow.Extensions;
using EventFlow.Logs;
using EventFlow.Snapshots;
using EventFlow.Snapshots.Stores;
using Microsoft.Azure.Cosmos.Table;


namespace EventFlow.AzureStorage.SnapshotStores
{
	public class AzureStorageSnapshotPersistence : ISnapshotPersistence
	{
		private readonly ILog _log;
		private readonly IAzureStorageFactory _azureStorageFactory;

		public AzureStorageSnapshotPersistence(ILog log, IAzureStorageFactory azureStorageFactory)
		{
			_log = log ?? throw new ArgumentNullException(nameof(log));
			_azureStorageFactory = azureStorageFactory ?? throw new ArgumentNullException(nameof(azureStorageFactory));
		}

		public Task<CommittedSnapshot> GetSnapshotAsync(Type aggregateType, IIdentity identity, CancellationToken cancellationToken)
			=> throw new NotImplementedException();

		public async Task SetSnapshotAsync(
			Type aggregateType,
			IIdentity identity,
			SerializedSnapshot serializedSnapshot,
			CancellationToken cancellationToken)
		{
			var (partitionKey, rowKey) = GetKeys(aggregateType, identity,serializedSnapshot.Metadata.AggregateSequenceNumber);
			var entity = new SnapshotEntity(partitionKey, rowKey)
				{
					AggregateName = partitionKey,
					AggregateId = identity.Value,
					AggregateSequenceNumber = serializedSnapshot.Metadata.AggregateSequenceNumber,
					Data = serializedSnapshot.SerializedData,
					Metadata = serializedSnapshot.SerializedMetadata
				};
			var operation = TableOperation.InsertOrReplace(entity);
			var table = _azureStorageFactory.CreateTableReferenceForSnapshotStore();
			await table.ExecuteAsync(operation, cancellationToken).ConfigureAwait(false);
		}

		public Task DeleteSnapshotAsync(Type aggregateType, IIdentity identity, CancellationToken cancellationToken)
			=> throw new NotImplementedException();

		public Task PurgeSnapshotsAsync(Type aggregateType, CancellationToken cancellationToken)
			=> throw new NotImplementedException();

		public Task PurgeSnapshotsAsync(CancellationToken cancellationToken)
			=> throw new NotImplementedException();
	
		private static (string partitionKey, string rowKey) GetKeys(Type aggregateType, IIdentity aggregateIdentity, int aggregateSequenceNumber)
			=> (GetPartitionKey(aggregateType), GetRowKey(aggregateIdentity, aggregateSequenceNumber));

		internal static string GetPartitionKey(Type aggregateType)
			=> aggregateType.GetAggregateName().Value;

		/// <summary>
		/// The row key is a combination of the aggregate's identity and the sequence number
		/// the snapshot is based on. The two parameters are separated by a <c>_</c> and
		/// the sequence number is padded with leading zeros to enable efficient sorting
		/// and querying.
		/// </summary>
		internal static string GetRowKey(IIdentity aggregateIdentity, int aggregateSequenceNumber)
			=> $"{aggregateIdentity.Value}_{aggregateSequenceNumber:D10}";


		internal class SnapshotEntity : TableEntity
		{
			public SnapshotEntity()
			{}

			public SnapshotEntity(string partitionKey, string rowKey)
				: base(partitionKey, rowKey)
			{}

			public string AggregateName { get; set; }
			public string AggregateId { get; set; }
			public int AggregateSequenceNumber { get; set; }
			public string Data { get; set; }
			public string Metadata { get; set; }
		}
	}
}