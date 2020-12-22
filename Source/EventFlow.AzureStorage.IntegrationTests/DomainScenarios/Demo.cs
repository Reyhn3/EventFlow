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
//TODO: Move the configuration into the UseAzureStorage.
				.ConfigureAzureStorage(new AzureStorageConfiguration
					{
						StorageAccountConnectionString = "UseDevelopmentStorage=true",
						SystemContainerName = "eventflow-system-params-demo",
						SequenceNumberRangeSize = 100,
						SequenceNumberOptimisticConcurrencyRetries = 25,
						EventStoreTableName = "EventFlowEventsDEMO",
						ReadStoreTableName = "EventFlowReadModelsDEMO",
						SnapshotStoreTableName = "EventFlowSnapshotsDEMO"
					})
				.CreateResolver();

			CommandBus = resolver.Resolve<ICommandBus>();
			QueryProcessor = resolver.Resolve<IQueryProcessor>();
		}
	}
}