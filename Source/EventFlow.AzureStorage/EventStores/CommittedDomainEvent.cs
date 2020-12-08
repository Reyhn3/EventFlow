using EventFlow.EventStores;


namespace EventFlow.AzureStorage.EventStores
{
	public class CommittedDomainEvent : ICommittedDomainEvent
	{
		public CommittedDomainEvent(string aggregateId, string data, string metadata, int aggregateSequenceNumber)
		{
			AggregateId = aggregateId;
			Data = data;
			Metadata = metadata;
			AggregateSequenceNumber = aggregateSequenceNumber;
		}

		public string AggregateId { get; }
		public string Data { get; }
		public string Metadata { get; }
		public int AggregateSequenceNumber { get; }
	}
}