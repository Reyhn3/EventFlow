using System;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.AzureStorage.IntegrationTests.Domain;
using EventFlow.AzureStorage.IntegrationTests.Domain.Queries;
using EventFlow.TestHelpers;
using NUnit.Framework;
using Shouldly;


namespace EventFlow.AzureStorage.IntegrationTests.DomainScenarios
{
	[Explicit("Intended for demonstration purposes")]
	[Category(Categories.Integration)]
	internal class QueryOnlyDemo : Demo
	{
		private static readonly FundId AggregateId = new FundId("a");

		[Test]
		public async Task Query_before_events_are_loaded_should_not_throw_exception()
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