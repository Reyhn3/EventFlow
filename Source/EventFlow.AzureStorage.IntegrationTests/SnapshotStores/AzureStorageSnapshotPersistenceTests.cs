using System.Threading;
using System.Threading.Tasks;
using EventFlow.Aggregates;
using EventFlow.AzureStorage.Config;
using EventFlow.AzureStorage.IntegrationTests.Domain;
using EventFlow.AzureStorage.SnapshotStores;
using EventFlow.Extensions;
using EventFlow.Snapshots;
using EventFlow.Snapshots.Stores;
using FakeItEasy;
using NUnit.Framework;
using Shouldly;


namespace EventFlow.AzureStorage.IntegrationTests.SnapshotStores
{
	[Explicit("Intended for manual verification")]
	[NonParallelizable]
	public class AzureStorageSnapshotPersistenceTests : IntegrationTests
	{
		private AzureStorageSnapshotPersistence _target;

		public AzureStorageSnapshotPersistenceTests()
			: base(o =>
				{
					o.UseAzureStorageSnapshotStore();
					o.UseInMemoryReadStoreFor<FundReadModel>();
				})
		{}

		[SetUp]
		public void PreRun()
		{
			var target = Resolver.Resolve<ISnapshotPersistence>();
			target.ShouldBeOfType<AzureStorageSnapshotPersistence>();
			_target = target as AzureStorageSnapshotPersistence;
		}

		[Test(Description = "The expected outcome is multiple snapshots for the same aggregate")]
		public async Task SetSnapshotAsync_should_write_snapshots_to_the_designated_table()
		{
			var id = new FundId("test-fund-a");

			var snapshot1 = new SerializedSnapshot("test-metadata", "test-data-v1", new SnapshotMetadata
				{
					AggregateSequenceNumber = 1
				});
			await _target.SetSnapshotAsync(typeof(FundAggregate), id, snapshot1, CancellationToken.None);

			var snapshot2 = new SerializedSnapshot("test-metadata", "test-data-v2", new SnapshotMetadata
				{
					AggregateSequenceNumber = 2
				});
			await _target.SetSnapshotAsync(typeof(FundAggregate), id, snapshot2, CancellationToken.None);
		}

		[Test(Description = "The expected outcome is a single snapshot containing the most recent data")]
		public async Task SetSnapshotAsync_should_overwrite_existing_snapshot_with_matching_identifiers()
		{
			var id = new FundId("test-fund-b");

			var snapshot1 = new SerializedSnapshot("test-metadata", "test-data-v1", new SnapshotMetadata
				{
					AggregateSequenceNumber = 1
				});
			await _target.SetSnapshotAsync(typeof(FundAggregate), id, snapshot1, CancellationToken.None);

			// This represents the same snapshot revision, but with modified data.
			var snapshot2 = new SerializedSnapshot("test-metadata", "test-data-v1-modified", new SnapshotMetadata
				{
					AggregateSequenceNumber = 1
				});
			await _target.SetSnapshotAsync(typeof(FundAggregate), id, snapshot2, CancellationToken.None);
		}

		[Test]
		public async Task GetSnapshotAsync_should_retrieve_the_latest_snapshot()
		{
			var aggregateType = typeof(FundAggregate);
			var id = new FundId("test-fund-latest");

			// Arrange

			var snapshot1 = new SerializedSnapshot("test-metadata", "test-data-v1", new SnapshotMetadata
				{
					AggregateSequenceNumber = 1
				});
			await _target.SetSnapshotAsync(aggregateType, id, snapshot1, CancellationToken.None);

			var snapshot2 = new SerializedSnapshot("test-metadata", "test-data-v2", new SnapshotMetadata
				{
					AggregateSequenceNumber = 2
				});
			await _target.SetSnapshotAsync(aggregateType, id, snapshot2, CancellationToken.None);


			// Act

			var result = await _target.GetSnapshotAsync(typeof(FundAggregate), id, A.Dummy<CancellationToken>());


			// Assert

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

			var snapshot1 = new SerializedSnapshot("test-metadata", "test-data-v1", new SnapshotMetadata
				{
					AggregateSequenceNumber = 1
				});
			await _target.SetSnapshotAsync(aggregateType, identityA, snapshot1, CancellationToken.None);

			var snapshot2 = new SerializedSnapshot("test-metadata", "test-data-v2", new SnapshotMetadata
				{
					AggregateSequenceNumber = 2
				});
			await _target.SetSnapshotAsync(aggregateType, identityA, snapshot2, CancellationToken.None);

			var snapshot3 = new SerializedSnapshot("test-metadata", "test-data-v1", new SnapshotMetadata
				{
					AggregateSequenceNumber = 1
				});
			await _target.SetSnapshotAsync(aggregateType, identityB, snapshot3, CancellationToken.None);

			var confirmSetupA = await _target.GetSnapshotAsync(aggregateType, identityA, A.Dummy<CancellationToken>());
			confirmSetupA.ShouldNotBeNull();
			var confirmSetupB = await _target.GetSnapshotAsync(aggregateType, identityB, A.Dummy<CancellationToken>());
			confirmSetupB.ShouldNotBeNull();


			// Act

			await _target.DeleteSnapshotAsync(aggregateType, identityA, CancellationToken.None);


			// Assert

			var resultA = await _target.GetSnapshotAsync(aggregateType, identityA, A.Dummy<CancellationToken>());
			resultA.ShouldBeNull();
			var resultB = await _target.GetSnapshotAsync(aggregateType, identityB, A.Dummy<CancellationToken>());
			resultB.ShouldNotBeNull();
		}

		[Test]
		public async Task PurgeSnapshotsAsync_shall_delete_all_snapshots_for_the_specified_aggregate()
		{
			var dummyAggregate = A.Fake<IAggregateRoot>();
			var aggregateType = dummyAggregate.GetType();
			var identityA = new FundId("test-fund-purge-a");
			var identityB = new FundId("test-fund-purge-b");


			// Arrange

			var snapshot1 = new SerializedSnapshot("test-metadata", "test-data-v1", new SnapshotMetadata
				{
					AggregateSequenceNumber = 1
				});
			await _target.SetSnapshotAsync(aggregateType, identityA, snapshot1, CancellationToken.None);

			var snapshot2 = new SerializedSnapshot("test-metadata", "test-data-v1", new SnapshotMetadata
				{
					AggregateSequenceNumber = 1
				});
			await _target.SetSnapshotAsync(aggregateType, identityB, snapshot2, CancellationToken.None);

			var confirmSetupA = await _target.GetSnapshotAsync(aggregateType, identityA, A.Dummy<CancellationToken>());
			confirmSetupA.ShouldNotBeNull();
			var confirmSetupB = await _target.GetSnapshotAsync(aggregateType, identityB, A.Dummy<CancellationToken>());
			confirmSetupB.ShouldNotBeNull();


			// Act

			await _target.PurgeSnapshotsAsync(aggregateType, CancellationToken.None);


			// Assert

			var resultA = await _target.GetSnapshotAsync(aggregateType, identityA, A.Dummy<CancellationToken>());
			resultA.ShouldBeNull();
			var resultB = await _target.GetSnapshotAsync(aggregateType, identityB, A.Dummy<CancellationToken>());
			resultB.ShouldBeNull();
		}

		[Test]
		public async Task PurgeSnapshotsAsync_shall_delete_all_snapshots()
		{
			var dummyAggregate = A.Fake<IAggregateRoot>();
			var aggregateTypeA = typeof(FundAggregate);
			var aggregateTypeB = dummyAggregate.GetType();
			var identityA = new FundId("test-fund-purgeall-a");
			var identityB1 = new FundId("test-fund-purgeall-b");
			var identityB2 = new FundId("test-fund-purgeall-c");


			// Arrange

			var snapshot1 = new SerializedSnapshot("test-metadata", "test-data-v1", new SnapshotMetadata
				{
					AggregateSequenceNumber = 1
				});
			await _target.SetSnapshotAsync(aggregateTypeA, identityA, snapshot1, CancellationToken.None);

			var snapshot2 = new SerializedSnapshot("test-metadata", "test-data-v1", new SnapshotMetadata
				{
					AggregateSequenceNumber = 1
				});
			await _target.SetSnapshotAsync(aggregateTypeB, identityB1, snapshot2, CancellationToken.None);

			var snapshot3 = new SerializedSnapshot("test-metadata", "test-data-v1", new SnapshotMetadata
				{
					AggregateSequenceNumber = 1
				});
			await _target.SetSnapshotAsync(aggregateTypeB, identityB2, snapshot3, CancellationToken.None);

			var confirmSetupA = await _target.GetSnapshotAsync(aggregateTypeA, identityA, A.Dummy<CancellationToken>());
			confirmSetupA.ShouldNotBeNull();
			var confirmSetupB1 = await _target.GetSnapshotAsync(aggregateTypeB, identityB1, A.Dummy<CancellationToken>());
			confirmSetupB1.ShouldNotBeNull();
			var confirmSetupB2 = await _target.GetSnapshotAsync(aggregateTypeB, identityB2, A.Dummy<CancellationToken>());
			confirmSetupB2.ShouldNotBeNull();


			// Act

			await _target.PurgeSnapshotsAsync(CancellationToken.None);


			// Assert

			var resultA = await _target.GetSnapshotAsync(aggregateTypeA, identityA, A.Dummy<CancellationToken>());
			resultA.ShouldBeNull();
			var resultB1 = await _target.GetSnapshotAsync(aggregateTypeB, identityB1, A.Dummy<CancellationToken>());
			resultB1.ShouldBeNull();
			var resultB2 = await _target.GetSnapshotAsync(aggregateTypeB, identityB2, A.Dummy<CancellationToken>());
			resultB2.ShouldBeNull();
		}
	}
}