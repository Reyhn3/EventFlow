using EventFlow.Aggregates;
using EventFlow.AzureStorage.IntegrationTests.Domain.Events;
using EventFlow.ReadStores;


namespace EventFlow.AzureStorage.IntegrationTests.Domain
{
	internal class FundReadModel : IReadModel,
		IAmReadModelFor<FundAggregate, FundId, FundSharesBought>,
		IAmReadModelFor<FundAggregate, FundId, FundSharesSold>
	{
		public decimal Quantity { get; private set; }

		public void Apply(IReadModelContext context, IDomainEvent<FundAggregate, FundId, FundSharesBought> domainEvent)
		{
			Quantity += domainEvent.AggregateEvent.Quantity.Value;
		}

		public void Apply(IReadModelContext context, IDomainEvent<FundAggregate, FundId, FundSharesSold> domainEvent)
		{
			Quantity -= domainEvent.AggregateEvent.Quantity.Value;
		}
	}
}