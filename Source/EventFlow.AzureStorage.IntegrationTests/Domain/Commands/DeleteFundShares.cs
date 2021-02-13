using System.Threading;
using System.Threading.Tasks;
using EventFlow.Aggregates.ExecutionResults;
using EventFlow.Commands;


namespace EventFlow.AzureStorage.IntegrationTests.Domain.Commands
{
	internal class DeleteFundShares : Command<FundAggregate, FundId, IExecutionResult>
	{
		public DeleteFundShares(FundId aggregateId)
			: base(aggregateId)
		{}
	}


	internal class DeleteFundSharesCommandHandler : CommandHandler<FundAggregate, FundId, IExecutionResult, DeleteFundShares>
	{
		public override Task<IExecutionResult> ExecuteCommandAsync(FundAggregate aggregate, DeleteFundShares command, CancellationToken cancellationToken)
		{
			var executionResult = aggregate.Delete();
			return Task.FromResult(executionResult);
		}
	}
}