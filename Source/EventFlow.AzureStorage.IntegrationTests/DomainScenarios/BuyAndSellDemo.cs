using System;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.AzureStorage.IntegrationTests.Domain;
using EventFlow.AzureStorage.IntegrationTests.Domain.Commands;
using EventFlow.AzureStorage.IntegrationTests.Domain.Queries;
using EventFlow.AzureStorage.IntegrationTests.Domain.ValueObjects;
using NUnit.Framework;
using Shouldly;


namespace EventFlow.AzureStorage.IntegrationTests.DomainScenarios
{
	internal class BuyAndSellDemo : Demo
	{
		private static readonly FundId AggregateId = new FundId("a");

		[Test]
		public async Task Buy_and_sell_funds()
		{
			Console.WriteLine("Buying 3 shares");
			var resultBuy = await CommandBus
				.PublishAsync(new BuyFundShares(AggregateId, new FundQuantity(3)), CancellationToken.None)
				.ConfigureAwait(false);
			resultBuy.IsSuccess.ShouldBeTrue();

			Console.WriteLine("Selling 2 shares");
			var resultSell = await CommandBus
				.PublishAsync(new SellFundShares(AggregateId, new FundQuantity(2)), CancellationToken.None)
				.ConfigureAwait(false);
			resultSell.IsSuccess.ShouldBeTrue();

			Console.WriteLine("Querying quantity");
			var resultQuery = await QueryProcessor
				.ProcessAsync(new GetCurrentFundShareQuantity(AggregateId), CancellationToken.None)
				.ConfigureAwait(false);
			Console.WriteLine("Result: {0}", resultQuery);
			resultQuery.ShouldBe(1);
		}
	}
}