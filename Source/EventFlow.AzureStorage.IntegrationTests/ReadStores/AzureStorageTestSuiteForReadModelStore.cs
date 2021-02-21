using System;
using System.Threading.Tasks;
using EventFlow.AzureStorage.Config;
using EventFlow.AzureStorage.Connection;
using EventFlow.AzureStorage.EventStores;
using EventFlow.AzureStorage.IntegrationTests.ReadStores.QueryHandlers;
using EventFlow.AzureStorage.IntegrationTests.ReadStores.ReadModels;
using EventFlow.Configuration;
using EventFlow.Extensions;
using EventFlow.TestHelpers;
using EventFlow.TestHelpers.Aggregates.Entities;
using EventFlow.TestHelpers.Suites;
using NUnit.Framework;


namespace EventFlow.AzureStorage.IntegrationTests.ReadStores
{
	[Category(Categories.Integration)]
	public class AzureStorageTestSuiteForReadModelStore : TestSuiteForReadModelStore
	{
		protected override Type ReadModelType { get; } = typeof(AzureStorageThingyReadModel);

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
				.UseAzureStorageReadModelFor<AzureStorageThingyReadModel>()
                .UseAzureStorageReadModelFor<AzureStorageThingyMessageReadModel, ThingyMessageLocator>()
				.AddQueryHandlers(
					typeof(AzureStorageThingyGetMessagesQueryHandler),
					typeof(AzureStorageThingyGetQueryHandler),
					typeof(AzureStorageThingyGetVersionQueryHandler))
                .RegisterServices(sr => { sr.RegisterType(typeof(ThingyMessageLocator)); })
				.CreateResolver();

		protected override Task PreRun()
			=> ResetAsync();

		[TearDown]
		public async Task PostRun() =>
			await ResetAsync();

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