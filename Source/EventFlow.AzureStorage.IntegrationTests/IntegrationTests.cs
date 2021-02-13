using System;
using System.Threading.Tasks;
using EventFlow.AzureStorage.Config;
using EventFlow.AzureStorage.Connection;
using EventFlow.AzureStorage.EventStores;
using EventFlow.AzureStorage.IntegrationTests.Domain;
using EventFlow.Configuration;
using EventFlow.Queries;
using EventFlow.TestHelpers;
using NUnit.Framework;


namespace EventFlow.AzureStorage.IntegrationTests
{
	/*
	 * In order to run these demos locally,
	 * you need to have the Azure Storage Emulator running.
	 * See:
	 * https://docs.microsoft.com/en-us/azure/storage/common/storage-use-emulator
	 */
	[Category(Categories.Integration)]
	public abstract class IntegrationTests
	{
		private readonly Action<IEventFlowOptions> _configure;

		protected IntegrationTests(Action<IEventFlowOptions> configure)
		{
			_configure = configure ?? (options => {});
		}

		protected IRootResolver Resolver { get; private set; }
		protected ICommandBus CommandBus { get; private set; }
		protected IQueryProcessor QueryProcessor { get; private set; }

		[OneTimeSetUp]
		public async Task SuitePreRun()
		{
			var eventFlowOptions = EventFlowOptions.New
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
					});

			// Apply any test specific configuration
			_configure(eventFlowOptions);

			Resolver = eventFlowOptions.CreateResolver();

			// Purge tables so all tests runs will be identical
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