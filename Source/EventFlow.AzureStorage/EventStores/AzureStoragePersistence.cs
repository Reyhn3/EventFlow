using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.AzureStorage.Connection;
using EventFlow.Core;
using EventFlow.EventStores;
using EventFlow.Logs;
using Microsoft.Azure.Cosmos.Table;


namespace EventFlow.AzureStorage.EventStores
{
	public class AzureStoragePersistence : IEventPersistence
	{
		private readonly ILog _log;
		private readonly IAzureStorageFactory _factory;

		public AzureStoragePersistence(ILog log, IAzureStorageFactory factory)
		{
			_log = log ?? throw new ArgumentNullException(nameof(log));
			_factory = factory ?? throw new ArgumentNullException(nameof(factory));
		}

		public async Task<AllCommittedEventsPage> LoadAllCommittedEvents(
			GlobalPosition globalPosition, 
			int pageSize, 
			CancellationToken cancellationToken)
		{
			throw new NotImplementedException();
		}

		public async Task<IReadOnlyCollection<ICommittedDomainEvent>> CommitEventsAsync(
			IIdentity id,
			IReadOnlyCollection<SerializedEvent> serializedEvents,
			CancellationToken cancellationToken)
		{
			throw new NotImplementedException();
		}

		public async Task<IReadOnlyCollection<ICommittedDomainEvent>> LoadCommittedEventsAsync(
			IIdentity id,
			int fromEventSequenceNumber,
			CancellationToken cancellationToken)
		{
			throw new NotImplementedException();
		}

		public async Task DeleteEventsAsync(IIdentity id, CancellationToken cancellationToken)
		{
			throw new NotImplementedException();
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