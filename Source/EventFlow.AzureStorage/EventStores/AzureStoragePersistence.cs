using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.Aggregates;
using EventFlow.AzureStorage.Connection;
using EventFlow.Core;
using EventFlow.EventStores;
using EventFlow.Logs;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.Cosmos.Table.Protocol;


namespace EventFlow.AzureStorage.EventStores
{
	public class AzureStoragePersistence : IEventPersistence
	{
		/// <summary>
		///     The row key is an int, and its <see cref="int.MaxValue" /> is
		///     ten digits long. To enable efficient sorting and querying by
		///     range, pad with leading zeros.
		/// </summary>
		private const string RowKeyFormatString = "D10";

		private readonly ILog _log;
		private readonly IAzureStorageFactory _factory;
		private readonly IUniqueIdGenerator _uniqueIdGenerator;

		public AzureStoragePersistence(ILog log, IAzureStorageFactory factory, IUniqueIdGenerator uniqueIdGenerator)
		{
			_log = log ?? throw new ArgumentNullException(nameof(log));
			_factory = factory ?? throw new ArgumentNullException(nameof(factory));
			_uniqueIdGenerator = uniqueIdGenerator ?? throw new ArgumentNullException(nameof(uniqueIdGenerator));
		}

		public async Task<AllCommittedEventsPage> LoadAllCommittedEvents(
			GlobalPosition globalPosition, 
			int pageSize, 
			CancellationToken cancellationToken)
		{
			var table = _factory.CreateTableReferenceForEventStore();
			var results = new List<EventDataEntity>();
			TableQuery<EventDataEntity> query;

			if (globalPosition.IsStart)
			{
				query = new TableQuery<EventDataEntity>().Take(pageSize);
			}
			else
			{
				var startPosition = long.Parse(globalPosition.Value);
				var filter = TableQuery.GenerateFilterConditionForLong(nameof(EventDataEntity.GlobalSequenceNumber), QueryComparisons.GreaterThanOrEqual, startPosition);
				query = new TableQuery<EventDataEntity>().Where(filter).Take(pageSize);
			}

			TableContinuationToken token = null;
			do
			{
				var resultSegment = await table.ExecuteQuerySegmentedAsync(query, token, cancellationToken).ConfigureAwait(false);
				token = resultSegment.ContinuationToken;

				results.AddRange(resultSegment.Results);
			} while (token != null);

			var nextPosition = results.Any()
				? results.Max(e => e.GlobalSequenceNumber) + 1
				: 0;

			var events = results
				.OrderBy(m => m.GlobalSequenceNumber)
				.Select(m => m.ToDomainEvent())
				.ToList()
				.AsReadOnly();

			return new AllCommittedEventsPage(new GlobalPosition(nextPosition.ToString()), events);
		}

		public async Task<IReadOnlyCollection<ICommittedDomainEvent>> CommitEventsAsync(
			IIdentity id,
			IReadOnlyCollection<SerializedEvent> serializedEvents,
			CancellationToken cancellationToken)
		{
			if (!serializedEvents.Any())
				return Array.Empty<ICommittedDomainEvent>();

			var entityTask = await Task.WhenAll(serializedEvents
					.Select(async e => new EventDataEntity(id.Value, e.AggregateSequenceNumber.ToString(RowKeyFormatString))
						{
							EventName = e.Metadata.EventName,
							AggregateId = id.Value,
							AggregateName = e.Metadata[MetadataKeys.AggregateName],
							AggregateSequenceNumber = e.AggregateSequenceNumber,
							Data = e.SerializedData,
							Metadata = e.SerializedMetadata,
							GlobalSequenceNumber = await _uniqueIdGenerator.GetNextIdAsync().ConfigureAwait(false),
							BatchId = Guid.Parse(e.Metadata[MetadataKeys.BatchId]),
						}))
				.ConfigureAwait(false);
			var entities = entityTask.ToArray();
			
			_log.Verbose("Committing {0} events to Azure Storage event store for entity with ID '{1}'", serializedEvents.Count, id);

			var operation = new TableBatchOperation();
			foreach (var entity in entities)
				operation.Add(TableOperation.Insert(entity));

			var table = _factory.CreateTableReferenceForEventStore();
			await table.ExecuteBatchAsync(operation, cancellationToken).ConfigureAwait(false);

			return entities;
		}

		public async Task<IReadOnlyCollection<ICommittedDomainEvent>> LoadCommittedEventsAsync(
			IIdentity id,
			int fromEventSequenceNumber,
			CancellationToken cancellationToken)
		{
			var partitionKey = id.Value;
			var partitionKeyFilter = TableQuery.GenerateFilterCondition(TableConstants.PartitionKey, QueryComparisons.Equal, partitionKey);

			var rowKeyStart = fromEventSequenceNumber.ToString(RowKeyFormatString);
			var rowKeyFilter = TableQuery.GenerateFilterCondition(TableConstants.RowKey, QueryComparisons.GreaterThanOrEqual, rowKeyStart);

			var filter = TableQuery.CombineFilters(partitionKeyFilter, TableOperators.And, rowKeyFilter);
			var query = new TableQuery<EventDataEntity>().Where(filter);
			var table = _factory.CreateTableReferenceForEventStore();
			var results = new List<ICommittedDomainEvent>();

			TableContinuationToken token = null;
			do
			{
				var resultSegment = await table.ExecuteQuerySegmentedAsync(query, token, cancellationToken).ConfigureAwait(false);
				token = resultSegment.ContinuationToken;

				results.AddRange(resultSegment.Results.Select(r => r.ToDomainEvent()));
			} while (token != null);

			return results.OrderBy(e => e.AggregateSequenceNumber).ToList().AsReadOnly();
		}

		public async Task DeleteEventsAsync(IIdentity id, CancellationToken cancellationToken)
		{
			var filter = TableQuery.GenerateFilterCondition(TableConstants.PartitionKey, QueryComparisons.Equal, id.Value);
			var query = new TableQuery().Where(filter).Select(new[] {TableConstants.RowKey});
			var table = _factory.CreateTableReferenceForEventStore();

			TableContinuationToken token = null;
			do
			{
				var resultSegment = await table.ExecuteQuerySegmentedAsync(query, token, cancellationToken).ConfigureAwait(false);
				token = resultSegment.ContinuationToken;

				var chunks = resultSegment.Results
					.Select((x, index) => new { Index = index, Value = x })
					.Where(x => x.Value != null)
					.GroupBy(x => x.Index / TableConstants.TableServiceBatchMaximumOperations)
					.Select(x => x.Select(v => v.Value));

				foreach (var chunk in chunks)
				{
					var operation = new TableBatchOperation();
					foreach (var entity in chunk)
						operation.Delete(entity);

					await table.ExecuteBatchAsync(operation, cancellationToken).ConfigureAwait(false);
				}
			} while (token != null);
		}


		private class EventDataEntity : TableEntity, ICommittedDomainEvent
		{
			public EventDataEntity()
			{}

			public EventDataEntity(string partitionKey, string rowKey)
				: base(partitionKey, rowKey)
			{}

			public string EventName { get; set; }
			public string AggregateId { get; set; }
			public string AggregateName { get; set; }
			public int AggregateSequenceNumber { get; set; }
			public string Data { get; set; }
			public string Metadata { get; set; }
			public long GlobalSequenceNumber { get; set; }
			public Guid BatchId { get; set; }

			public CommittedDomainEvent ToDomainEvent()
			{
				return new CommittedDomainEvent(AggregateId, Data, Metadata, AggregateSequenceNumber);
			}
		}
	}
}