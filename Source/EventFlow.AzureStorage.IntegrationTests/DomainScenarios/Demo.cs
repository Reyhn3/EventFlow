using EventFlow.AzureStorage.IntegrationTests.Domain;
using EventFlow.Extensions;
using EventFlow.Queries;
using EventFlow.TestHelpers;
using NUnit.Framework;


namespace EventFlow.AzureStorage.IntegrationTests.DomainScenarios
{
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
				.UseInMemoryReadStoreFor<FundReadModel>()
				.CreateResolver();

			CommandBus = resolver.Resolve<ICommandBus>();
			QueryProcessor = resolver.Resolve<IQueryProcessor>();
		}
	}
}