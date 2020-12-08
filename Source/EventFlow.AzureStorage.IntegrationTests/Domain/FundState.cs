using EventFlow.Aggregates;
using EventFlow.AzureStorage.IntegrationTests.Domain.Events;


namespace EventFlow.AzureStorage.IntegrationTests.Domain
{
	internal class FundState : AggregateState<FundAggregate, FundId, FundState>,
		IApply<FundSharesBought>,
		IApply<FundSharesSold>
	{
		public decimal Quantity { get; private set; }

		public void Apply(FundSharesBought aggregateEvent)
		{
			Quantity += aggregateEvent.Quantity.Value;
		}

		public void Apply(FundSharesSold aggregateEvent)
		{
			Quantity -= aggregateEvent.Quantity.Value;
		}
	}
}