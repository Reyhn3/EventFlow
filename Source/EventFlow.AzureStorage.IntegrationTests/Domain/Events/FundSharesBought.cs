using EventFlow.Aggregates;
using EventFlow.AzureStorage.IntegrationTests.Domain.ValueObjects;
using EventFlow.EventStores;


namespace EventFlow.AzureStorage.IntegrationTests.Domain.Events
{
	[EventVersion("FundSharesBought", 1)]
	internal class FundSharesBought : AggregateEvent<FundAggregate, FundId>
	{
		public FundSharesBought(FundQuantity quantity)
		{
			Quantity = quantity;
		}

		public FundQuantity Quantity { get; }
	}
}