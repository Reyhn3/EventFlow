using EventFlow.AzureStorage.Config;
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

		[SetUp]
		public void PreRun()
		{
			var resolver = EventFlowOptions.New
				.RegisterModule<Module>()
				.UseAzureStorage()
				.UseAzureStorageEventStore()
				.UseAzureStorageSnapshotStore()
				.UseAzureStorageReadModelFor<FundReadModel>()
				.ConfigureAzureStorage(c =>
					{
						c.StorageAccountConnectionString = "UseDevelopmentStorage=true";
						c.SystemContainerName = "eventflow-system-params-demo";
						c.SequenceNumberRangeSize = 100;
						c.SequenceNumberOptimisticConcurrencyRetries = 25;
						c.EventStoreTableName = "EventFlowEventsDEMO";
						c.ReadStoreTableName = "EventFlowReadModelsDEMO";
						c.SnapshotStoreTableName = "EventFlowSnapshotsDEMO";
					})
				.CreateResolver();

			CommandBus = resolver.Resolve<ICommandBus>();
			QueryProcessor = resolver.Resolve<IQueryProcessor>();
		}
	}
}