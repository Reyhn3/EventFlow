using System.Threading;
using System.Threading.Tasks;
using EventFlow.Aggregates.ExecutionResults;
using EventFlow.AzureStorage.IntegrationTests.Domain.ValueObjects;
using EventFlow.Commands;


namespace EventFlow.AzureStorage.IntegrationTests.Domain.Commands
{
	internal class SellFundShares : Command<FundAggregate, FundId, IExecutionResult>
	{
		public SellFundShares(FundId aggregateId, FundQuantity quantity)
			: base(aggregateId)
		{
			Quantity = quantity;
		}

		public FundQuantity Quantity { get; }
	}


	internal class SellFundSharesCommandHandler : CommandHandler<FundAggregate, FundId, IExecutionResult, SellFundShares>
	{
		public override Task<IExecutionResult> ExecuteCommandAsync(FundAggregate aggregate, SellFundShares command, CancellationToken cancellationToken)
		{
			var executionResult = aggregate.SellShares(command);
			return Task.FromResult(executionResult);
		}
	}
}