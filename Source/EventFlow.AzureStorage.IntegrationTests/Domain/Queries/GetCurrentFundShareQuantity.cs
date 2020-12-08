using System;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.Queries;
using EventFlow.ReadStores;


namespace EventFlow.AzureStorage.IntegrationTests.Domain.Queries
{
	internal class GetCurrentFundShareQuantity : IQuery<decimal>
	{
		public GetCurrentFundShareQuantity(FundId fundId)
		{
			FundId = fundId;
		}

		public FundId FundId { get; }
	}


	internal class GetCurrentFundShareQuantityQueryHandler : IQueryHandler<GetCurrentFundShareQuantity, decimal>
	{
		private readonly IReadModelStore<FundReadModel> _readStore;

		public GetCurrentFundShareQuantityQueryHandler(IReadModelStore<FundReadModel> readStore)
		{
			_readStore = readStore ?? throw new ArgumentNullException(nameof(readStore));
		}

		public async Task<decimal> ExecuteQueryAsync(GetCurrentFundShareQuantity query, CancellationToken cancellationToken)
		{
			var readModels = await _readStore.GetAsync(query.FundId, cancellationToken).ConfigureAwait(false);
			return readModels.ReadModel?.State.Quantity ?? decimal.Zero;
		}
	}
}