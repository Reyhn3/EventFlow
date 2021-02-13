using EventFlow.Aggregates;
using EventFlow.EventStores;


namespace EventFlow.AzureStorage.IntegrationTests.Domain.Events
{
	[EventVersion("FundSharesDeleted", 1)]
	internal class FundSharesDeleted : AggregateEvent<FundAggregate, FundId>
	{}
}