using EventFlow.Aggregates;
using EventFlow.AzureStorage.IntegrationTests.Domain.ValueObjects;
using EventFlow.EventStores;


namespace EventFlow.AzureStorage.IntegrationTests.Domain.Events
{
	[EventVersion("FundSharesSold", 1)]
	internal class FundSharesSold : AggregateEvent<FundAggregate, FundId>
	{
		public FundSharesSold(FundQuantity quantity)
		{
			Quantity = quantity;
		}

		public FundQuantity Quantity { get; }
	}
}