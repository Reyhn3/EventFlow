﻿using System.Threading.Tasks;
using EventFlow.AzureStorage.Config;
using EventFlow.AzureStorage.Connection;
using EventFlow.AzureStorage.EventStores;
using EventFlow.AzureStorage.IntegrationTests.Domain;
using EventFlow.Queries;
using EventFlow.TestHelpers;
using NUnit.Framework;


namespace EventFlow.AzureStorage.IntegrationTests.DomainScenarios
{
	/*
	 * In order to run these demos locally,
	 * you need to have the Azure Storage Emulator running.
	 * See:
	 * https://docs.microsoft.com/en-us/azure/storage/common/storage-use-emulator
	 */
	[Explicit("For manual demonstration purposes only")]
	[Category(Categories.Integration)]
	internal abstract class Demo
	{
		protected ICommandBus CommandBus { get; private set; }
		protected IQueryProcessor QueryProcessor { get; private set; }

		[OneTimeSetUp]
		public async Task SuitePreRun()
		{
			var resolver = EventFlowOptions.New
				.RegisterModule<Module>()
				.UseAzureStorage(c =>
					{
						c.StorageAccountConnectionString = "UseDevelopmentStorage=true";
						c.SystemContainerName = "eventflow-system-params-demo";
						c.SequenceNumberRangeSize = 100;
						c.SequenceNumberOptimisticConcurrencyRetries = 25;
						c.EventStoreTableName = "EventFlowEventsDEMO";
						c.ReadStoreTableName = "EventFlowReadModelsDEMO";
						c.SnapshotStoreTableName = "EventFlowSnapshotsDEMO";
					})
				.UseAzureStorageEventStore()
				.UseAzureStorageSnapshotStore()
				.UseAzureStorageReadModelFor<FundReadModel>()
				.CreateResolver();

			CommandBus = resolver.Resolve<ICommandBus>();
			QueryProcessor = resolver.Resolve<IQueryProcessor>();

			var azureStorageFactory = resolver.Resolve<IAzureStorageFactory>();
			await PurgeAllDemoTables(azureStorageFactory);
			var syncStore = resolver.Resolve<IOptimisticSyncStore>();
			await syncStore.TryOptimisticWriteAsync(0);
		}

		private static async Task PurgeAllDemoTables(IAzureStorageFactory azureStorageFactory)
		{
			await TableHelper.PurgeTable(azureStorageFactory.CreateTableReferenceForEventStore());
			await TableHelper.PurgeTable(azureStorageFactory.CreateTableReferenceForReadStore());
			await TableHelper.PurgeTable(azureStorageFactory.CreateTableReferenceForSnapshotStore());
		}
	}
}