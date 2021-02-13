using EventFlow.AzureStorage.IntegrationTests.Domain;
using FakeItEasy;


namespace EventFlow.AzureStorage.IntegrationTests
{
	internal class FundIdDummyFactory : DummyFactory<FundId>
	{
		protected override FundId Create() => new FundId("test-fund-id");
	}
}