using EventFlow.AzureStorage.Config;
using EventFlow.AzureStorage.Extensions;
using EventFlow.AzureStorage.IntegrationTests.Domain;
using EventFlow.Extensions;
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
				.UseAzureStorageReadModel<FundReadModel>()
				.UseAzureStorage()
				.UseAzureStorageEventStore()
				.ConfigureAzureStorage(new AzureStorageConfiguration
					{
						StorageAccountConnectionString = "UseDevelopmentStorage=true",
						SystemContainerName = "eventflow-system-params",
						SequenceNumberRangeSize = 100,
						SequenceNumberOptimisticConcurrencyRetries = 25,
						EventStoreTableName = "EventFlowEvents",
						ReadStoreTableName = "EventFlowReadModels"
					})
				.CreateResolver();

			CommandBus = resolver.Resolve<ICommandBus>();
			QueryProcessor = resolver.Resolve<IQueryProcessor>();
		}
	}
}