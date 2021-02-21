using EventFlow.Aggregates;
using EventFlow.ReadStores;
using EventFlow.TestHelpers.Aggregates;
using EventFlow.TestHelpers.Aggregates.Events;


namespace EventFlow.AzureStorage.IntegrationTests.ReadStores.ReadModels
{
	public class AzureStorageThingyReadModel : IReadModel,
		IAmReadModelFor<ThingyAggregate, ThingyId, ThingyDomainErrorAfterFirstEvent>,
		IAmReadModelFor<ThingyAggregate, ThingyId, ThingyPingEvent>,
		IAmReadModelFor<ThingyAggregate, ThingyId, ThingyDeletedEvent>
	{
		public string Id { get; set; }
		public bool DomainErrorAfterFirstReceived { get; set; }
		public int PingsReceived { get; set; }

		public void Apply(IReadModelContext context, IDomainEvent<ThingyAggregate, ThingyId, ThingyDeletedEvent> domainEvent)
		{
			context.MarkForDeletion();
		}

		public void Apply(IReadModelContext context, IDomainEvent<ThingyAggregate, ThingyId, ThingyDomainErrorAfterFirstEvent> domainEvent)
		{
			Id = domainEvent.AggregateIdentity.Value;
			DomainErrorAfterFirstReceived = true;
		}

		public void Apply(IReadModelContext context, IDomainEvent<ThingyAggregate, ThingyId, ThingyPingEvent> domainEvent)
		{
			Id = domainEvent.AggregateIdentity.Value;
			PingsReceived++;
		}

		public Thingy ToThingy() =>
			new Thingy(
				ThingyId.With(Id),
				PingsReceived,
				DomainErrorAfterFirstReceived);
	}
}