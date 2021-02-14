using EventFlow.AzureStorage.Config;
using EventFlow.AzureStorage.IntegrationTests.Domain;
using EventFlow.Configuration;
using EventFlow.TestHelpers;
using EventFlow.TestHelpers.Suites;
using NUnit.Framework;


namespace EventFlow.AzureStorage.IntegrationTests.EventStores
{
	[Category(Categories.Integration)]
	public class AzureStorageEventStoreTests : TestSuiteForEventStore
	{
		protected override IRootResolver CreateRootResolver(IEventFlowOptions eventFlowOptions)
		{
			var resolver = eventFlowOptions
				.RegisterModule<Module>()
				.UseAzureStorage(c =>
					{
						c.StorageAccountConnectionString = "UseDevelopmentStorage=true";
						c.SystemContainerName = "eventflow-system-params-test";
						c.SequenceNumberRangeSize = 100;
						c.SequenceNumberOptimisticConcurrencyRetries = 25;
						c.EventStoreTableName = "EventFlowEventsTEST";
						c.ReadStoreTableName = "EventFlowReadModelsTEST";
						c.SnapshotStoreTableName = "EventFlowSnapshotsTEST";
					})
				.UseAzureStorageEventStore()
				.UseAzureStorageReadModelFor<FundReadModel>()
				.CreateResolver();
			return resolver;
		}
	}
}