using System.Threading.Tasks;
using Castle.DynamicProxy.Generators.Emitters.SimpleAST;
using EventFlow.AzureStorage.Config;
using EventFlow.AzureStorage.Connection;
using EventFlow.AzureStorage.EventStores;
using EventFlow.Configuration;
using EventFlow.TestHelpers;
using EventFlow.TestHelpers.Suites;
using NUnit.Framework;


namespace EventFlow.AzureStorage.IntegrationTests.EventStores
{
	[Category(Categories.Integration)]
	public class AzureStorageTestSuiteForEventStore : TestSuiteForEventStore
	{
		protected override IRootResolver CreateRootResolver(IEventFlowOptions eventFlowOptions)
			=> eventFlowOptions
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
				.UseAzureStorageSnapshotStore()
				.CreateResolver();

		protected override Task PreRun()
			=> ResetAsync();

		[TearDown]
		public async Task PostRun()
		{
			await ResetAsync();
		}

		private async Task ResetAsync()
		{
			var azureStorageFactory = Resolver.Resolve<IAzureStorageFactory>();
			await PurgeAllTestTables(azureStorageFactory);
			var syncStore = Resolver.Resolve<IOptimisticSyncStore>();
			await syncStore.TryOptimisticWriteAsync(0);
		}

		private static async Task PurgeAllTestTables(IAzureStorageFactory azureStorageFactory)
		{
			await TableHelper.PurgeTable(azureStorageFactory.CreateTableReferenceForEventStore());
			await TableHelper.PurgeTable(azureStorageFactory.CreateTableReferenceForReadStore());
			await TableHelper.PurgeTable(azureStorageFactory.CreateTableReferenceForSnapshotStore());
		}
	}
}