using System.Threading;
using System.Threading.Tasks;
using EventFlow.AzureStorage.Config;
using EventFlow.AzureStorage.Extensions;
using EventFlow.AzureStorage.IntegrationTests.Domain;
using EventFlow.AzureStorage.SnapshotStores;
using EventFlow.Configuration;
using EventFlow.Extensions;
using EventFlow.Snapshots;
using EventFlow.Snapshots.Stores;
using EventFlow.TestHelpers;
using NUnit.Framework;
using Shouldly;


namespace EventFlow.AzureStorage.IntegrationTests.SnapshotStores
{
	[Explicit("Intended for manual verification")]
	[Category(Categories.Integration)]
	public class AzureStorageSnapshotPersistenceTests
	{
		private IRootResolver _resolver;
		private ISnapshotPersistence _target;

		[SetUp]
		public void PreRun()
		{
			_resolver = EventFlowOptions.New
				.RegisterModule<Module>()
				.UseAzureStorage()
				.UseAzureStorageSnapshotStore()
				.ConfigureAzureStorage(new AzureStorageConfiguration
					{
						StorageAccountConnectionString = "UseDevelopmentStorage=true",
						SnapshotStoreTableName = "EventFlowSnapshotsTEST"
					})
				.UseInMemoryReadStoreFor<FundReadModel>()
				.CreateResolver();

			_target = _resolver.Resolve<ISnapshotPersistence>();
			_target.ShouldBeOfType<AzureStorageSnapshotPersistence>();
		}

		[Test(Description = "The expected outcome is multiple snapshots for the same aggregate")]
		public async Task SetSnapshotAsync_should_write_snapshots_to_the_designated_table()
		{
			var snapshot1 = new SerializedSnapshot("test-metadata", "test-data-v1", new SnapshotMetadata {AggregateSequenceNumber = 1});
			await _target.SetSnapshotAsync(typeof(FundAggregate), new FundId("test-fund-a"), snapshot1, CancellationToken.None);

			var snapshot2 = new SerializedSnapshot("test-metadata", "test-data-v2", new SnapshotMetadata {AggregateSequenceNumber = 2});
			await _target.SetSnapshotAsync(typeof(FundAggregate), new FundId("test-fund-a"), snapshot2, CancellationToken.None);
		}

		[Test(Description = "The expected outcome is a single snapshot containing the most recent data")]
		public async Task SetSnapshotAsync_should_overwrite_existing_snapshot_with_matching_identifiers()
		{
			var snapshot1 = new SerializedSnapshot("test-metadata", "test-data-v1", new SnapshotMetadata {AggregateSequenceNumber = 1});
			await _target.SetSnapshotAsync(typeof(FundAggregate), new FundId("test-fund-b"), snapshot1, CancellationToken.None);

			// This represents the same snapshot revision, but with modified data.
			var snapshot2 = new SerializedSnapshot("test-metadata", "test-data-v1-modified", new SnapshotMetadata {AggregateSequenceNumber = 1});
			await _target.SetSnapshotAsync(typeof(FundAggregate), new FundId("test-fund-b"), snapshot2, CancellationToken.None);
		}

		[Test]
		public async Task GetSnapshotAsync_should_retrieve_the_latest_snapshot()
		{
			var result = await _target.GetSnapshotAsync(typeof(FundAggregate), new FundId("test-fund-a"), CancellationToken.None);
			result.ShouldNotBeNull();
			result.SerializedData.ShouldBe("test-data-v2");
		}

		[Test]
		public async Task DeleteSnapshotAsync_shall_delete_all_snapshots_for_the_specified_aggregate_and_id()
		{
			var aggregateType = typeof(FundAggregate);
			var identityA = new FundId("test-fund-delete-a");
			var identityB = new FundId("test-fund-delete-b");

			
			// Arrange
			var snapshot1 = new SerializedSnapshot("test-metadata", "test-data-v1", new SnapshotMetadata {AggregateSequenceNumber = 1});
			await _target.SetSnapshotAsync(aggregateType, identityA, snapshot1, CancellationToken.None);

			var snapshot2 = new SerializedSnapshot("test-metadata", "test-data-v2", new SnapshotMetadata {AggregateSequenceNumber = 2});
			await _target.SetSnapshotAsync(aggregateType, identityA, snapshot2, CancellationToken.None);

			var snapshot3 = new SerializedSnapshot("test-metadata", "test-data-v1", new SnapshotMetadata {AggregateSequenceNumber = 1});
			await _target.SetSnapshotAsync(aggregateType, identityB, snapshot3, CancellationToken.None);

			var confirmSetupA = await _target.GetSnapshotAsync(aggregateType, identityA, CancellationToken.None);
			confirmSetupA.ShouldNotBeNull();
			var confirmSetupB = await _target.GetSnapshotAsync(aggregateType, identityB, CancellationToken.None);
			confirmSetupB.ShouldNotBeNull();
			

			// Act
			await _target.DeleteSnapshotAsync(aggregateType, identityA, CancellationToken.None);
			
			
			// Assert
			var resultA = await _target.GetSnapshotAsync(aggregateType, identityA, CancellationToken.None);
			resultA.ShouldBeNull();
			var resultB = await _target.GetSnapshotAsync(aggregateType, identityB, CancellationToken.None);
			resultB.ShouldNotBeNull();
		}
	}
}