using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;


namespace EventFlow.AzureStorage.IntegrationTests
{
	internal static class TableHelper
	{
		// This is a naive way of deleting everything and would not hold up in production.
		// But the tests should only add a handful of entities so it should be OK.
		public static async Task PurgeTable(CloudTable table)
		{
			var query = new TableQuery<DynamicTableEntity>();
			var entities = new List<DynamicTableEntity>();

			TableContinuationToken token = null;
			do
			{
				var resultSegment = await table.ExecuteQuerySegmentedAsync(query, token);
				token = resultSegment.ContinuationToken;
				entities.AddRange(resultSegment.Results);
			} while (token != null);

			if (!entities.Any())
				return;

			var batches = entities.GroupBy(e => e.PartitionKey);
			foreach (var batch in batches)
			{
				var operation = new TableBatchOperation();
				foreach (var entity in batch)
					operation.Add(TableOperation.Delete(entity));

				await table.ExecuteBatchAsync(operation, CancellationToken.None);
			}
		}
	}
}