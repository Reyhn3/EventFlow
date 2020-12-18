using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.AzureStorage.Connection;
using EventFlow.Core;
using EventFlow.Extensions;
using EventFlow.Logs;
using EventFlow.Snapshots;
using EventFlow.Snapshots.Stores;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.Cosmos.Table.Protocol;


namespace EventFlow.AzureStorage.SnapshotStores
{
	public class AzureStorageSnapshotPersistence : ISnapshotPersistence
	{
		/// <summary>
		///     The row key is an int, and its <see cref="int.MaxValue" /> is
		///     ten digits long. To enable efficient sorting and querying by
		///     range, pad with leading zeros.
		/// </summary>
		private const string RowKeyFormatString = "D10";

		private readonly IAzureStorageFactory _azureStorageFactory;
		private readonly ILog _log;

		public AzureStorageSnapshotPersistence(ILog log, IAzureStorageFactory azureStorageFactory)
		{
			_log = log ?? throw new ArgumentNullException(nameof(log));
			_azureStorageFactory = azureStorageFactory ?? throw new ArgumentNullException(nameof(azureStorageFactory));
		}

		public Task<CommittedSnapshot> GetSnapshotAsync(Type aggregateType, IIdentity identity, CancellationToken cancellationToken)
		{
			var partitionKey = GetPartitionKey(aggregateType, identity);
			var query = new TableQuery<SnapshotEntity>()
				.Where(TableQuery.GenerateFilterCondition(TableConstants.PartitionKey, QueryComparisons.Equal, partitionKey))
				.Take(1); // Since the RowKey is naturally descending, only take the top 1 entity, which is the latest one.
			var table = _azureStorageFactory.CreateTableReferenceForSnapshotStore();
			var result = table.ExecuteQuery(query);
			var entity = result.SingleOrDefault();

			if (entity == null)
				return null;

			var snapshot = new CommittedSnapshot(entity.Metadata, entity.Data);
			return Task.FromResult(snapshot);
		}

		public async Task SetSnapshotAsync(
			Type aggregateType,
			IIdentity identity,
			SerializedSnapshot serializedSnapshot,
			CancellationToken cancellationToken)
		{
			var (partitionKey, rowKey) = GetKeys(aggregateType, identity, serializedSnapshot.Metadata.AggregateSequenceNumber);
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
			=> (GetPartitionKey(aggregateType, aggregateIdentity), GetRowKey(aggregateSequenceNumber));


		/// <summary>
		///     The partition key is a combination of the aggregate's type and identity, separated by a <c>_</c>.
		/// </summary>
		/// <param name="aggregateType">The type of the aggregate</param>
		/// <param name="aggregateIdentity">The identity of the aggregate</param>
		/// <returns>A compound partition key</returns>
		/// <remarks>
		///     This makes is a little more difficult for humans to read, but it will make querying the table
		///     much more efficient.
		/// </remarks>
		internal static string GetPartitionKey(Type aggregateType, IIdentity aggregateIdentity)
			=> $"{aggregateType.GetAggregateName().Value}_{aggregateIdentity.Value}";

		/// <summary>
		///     The row key is the aggregate sequence number in reverse order.
		/// </summary>
		/// <remarks>
		///     This is a trick for more efficient querying. Since the table is by default
		///     sorting strings in ascending order, having a row key that sorts naturally in
		///     descending order makes it easier to retrieve the latest snapshot.
		/// </remarks>
		/// <param name="aggregateSequenceNumber">The aggregate sequence number</param>
		/// <returns>A sortable row key</returns>
		//
		// WARNING: Changing this logic will make GetSnapshotAsync stop working!
		internal static string GetRowKey(int aggregateSequenceNumber)
			=> (int.MaxValue - aggregateSequenceNumber).ToString(RowKeyFormatString);


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