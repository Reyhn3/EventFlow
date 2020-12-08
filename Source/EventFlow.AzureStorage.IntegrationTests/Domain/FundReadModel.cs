using EventFlow.Aggregates;
using EventFlow.AzureStorage.IntegrationTests.Domain.Events;
using EventFlow.ReadStores;


namespace EventFlow.AzureStorage.IntegrationTests.Domain
{
	internal class FundReadModel : IReadModel,
		IAmReadModelFor<FundAggregate, FundId, FundSharesBought>,
		IAmReadModelFor<FundAggregate, FundId, FundSharesSold>
	{
		public FundState State { get; } = new FundState();

		public void Apply(IReadModelContext context, IDomainEvent<FundAggregate, FundId, FundSharesBought> domainEvent)
		{
			State.Apply(domainEvent.AggregateEvent);
		}

		public void Apply(IReadModelContext context, IDomainEvent<FundAggregate, FundId, FundSharesSold> domainEvent)
		{
			State.Apply(domainEvent.AggregateEvent);
		}
	}
}