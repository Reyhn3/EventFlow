using System.Threading;
using System.Threading.Tasks;
using EventFlow.Aggregates.ExecutionResults;
using EventFlow.AzureStorage.IntegrationTests.Domain.ValueObjects;
using EventFlow.Commands;


namespace EventFlow.AzureStorage.IntegrationTests.Domain.Commands
{
	internal class BuyFundShares : Command<FundAggregate, FundId, IExecutionResult>
	{
		public BuyFundShares(FundId aggregateId, FundQuantity quantity)
			: base(aggregateId)
		{
			Quantity = quantity;
		}

		public FundQuantity Quantity { get; }
	}


	internal class BuyFundSharesCommandHandler : CommandHandler<FundAggregate, FundId, IExecutionResult, BuyFundShares>
	{
		public override Task<IExecutionResult> ExecuteCommandAsync(FundAggregate aggregate, BuyFundShares command, CancellationToken cancellationToken)
		{
			var executionResult = aggregate.BuyShares(command);
			return Task.FromResult(executionResult);
		}
	}
}