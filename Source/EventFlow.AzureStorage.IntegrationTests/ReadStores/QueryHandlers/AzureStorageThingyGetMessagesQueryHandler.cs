using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.AzureStorage.IntegrationTests.ReadStores.ReadModels;
using EventFlow.Queries;
using EventFlow.ReadStores;
using EventFlow.TestHelpers.Aggregates.Entities;
using EventFlow.TestHelpers.Aggregates.Queries;


namespace EventFlow.AzureStorage.IntegrationTests.ReadStores.QueryHandlers
{
    public class AzureStorageThingyGetMessagesQueryHandler : IQueryHandler<ThingyGetMessagesQuery, IReadOnlyCollection<ThingyMessage>>
    {
	    private readonly IReadModelStore<AzureStorageThingyMessageReadModel> _readStore;

	    public AzureStorageThingyGetMessagesQueryHandler(IReadModelStore<AzureStorageThingyMessageReadModel> readStore)
	    {
		    _readStore = readStore ?? throw new ArgumentNullException(nameof(readStore));
	    }

        public async Task<IReadOnlyCollection<ThingyMessage>> ExecuteQueryAsync(ThingyGetMessagesQuery query, CancellationToken cancellationToken)
        {
	        var readModel = await _readStore.GetAsync(query.ThingyId.Value, cancellationToken).ConfigureAwait(false);
	        throw new NotImplementedException("Figure out how to read multiple entities");
        }
	}
}