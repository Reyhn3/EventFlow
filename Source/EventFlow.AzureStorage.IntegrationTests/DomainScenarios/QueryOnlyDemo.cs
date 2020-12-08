using System;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.AzureStorage.IntegrationTests.Domain;
using EventFlow.AzureStorage.IntegrationTests.Domain.Queries;
using NUnit.Framework;
using Shouldly;


namespace EventFlow.AzureStorage.IntegrationTests.DomainScenarios
{
	internal class QueryOnlyDemo : Demo
	{
		private static readonly FundId AggregateId = new FundId("a");

		[Test]
		public async Task Query_before_events_are_loaded()
		{
			Console.WriteLine("Querying quantity");
			var result = await QueryProcessor
				.ProcessAsync(new GetCurrentFundShareQuantity(AggregateId), CancellationToken.None)
				.ConfigureAwait(false);
			Console.WriteLine("Result: {0}", result);
			result.ShouldBe(0);
		}
	}
}