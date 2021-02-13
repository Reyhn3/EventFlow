using EventFlow.Aggregates;
using EventFlow.Aggregates.ExecutionResults;
using EventFlow.AzureStorage.IntegrationTests.Domain.Commands;
using EventFlow.AzureStorage.IntegrationTests.Domain.Events;
using EventFlow.AzureStorage.IntegrationTests.Domain.Specifications;
using EventFlow.Extensions;


namespace EventFlow.AzureStorage.IntegrationTests.Domain
{
	internal class FundAggregate : AggregateRoot<FundAggregate, FundId>
	{
		private readonly FundState _state = new FundState();

		public FundAggregate(FundId id)
			: base(id)
		{
			Register(_state);
		}

		internal IExecutionResult BuyShares(BuyFundShares command)
		{
			// This operation has no requirements to validate.

			Emit(new FundSharesBought(command.Quantity));
			return ExecutionResult.Success();
		}

		internal IExecutionResult SellShares(SellFundShares command)
		{
			// Validate the operation according to the following business rules.
			Specs.HasSufficientQuantity(command.Quantity.Value).ThrowDomainErrorIfNotSatisfied(command.Quantity.Value);

			Emit(new FundSharesSold(command.Quantity));
			return ExecutionResult.Success();
		}

		public IExecutionResult Delete()
		{
			Emit(new FundSharesDeleted());
			return ExecutionResult.Success();
		}
	}
}