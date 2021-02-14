using System;
using System.Threading.Tasks;
using EventFlow.AzureStorage.Connection;
using EventFlow.AzureStorage.EventStores;
using EventFlow.AzureStorage.IntegrationTests.Domain;
using EventFlow.Extensions;
using NUnit.Framework;
using Shouldly;


namespace EventFlow.AzureStorage.IntegrationTests.EventStores
{
	[Explicit("Intended for manual verification")]
	public class BlobOptimisticSyncStoreTests : IntegrationTests
	{
		private BlobOptimisticSyncStore _target;

		public BlobOptimisticSyncStoreTests()
			: base(o => { o.UseInMemoryReadStoreFor<FundReadModel>(); })
		{}

		[SetUp]
		public void PreRun()
		{
			var target = Resolver.Resolve<IOptimisticSyncStore>();
			target.ShouldBeOfType<BlobOptimisticSyncStore>();
			_target = target as BlobOptimisticSyncStore;
		}

		[Order(0)]
		[Test]
		public async Task InitializeAsync_should_create_container_and_blob_with_value_zero_if_it_does_not_exist()
		{
			await _target.InitializeAsync();
			var current = await _target.GetCurrentAsync();
			Console.WriteLine(current);
			current.ShouldBe(0);
		}

		[Order(1)]
		[Test(Description = "Set a specific value")]
		public async Task TryOptimisticWriteAsync_should_write_new_value()
		{
			var result = await _target.TryOptimisticWriteAsync(1);
			result.ShouldBeTrue();
		}

		[Order(2)]
		[Test(Description = "Read the current value")]
		public async Task GetDataAsync_should_retrieve_the_current_value()
		{
			var result = await _target.GetCurrentAsync();
			Console.WriteLine(result);
			result.ShouldBe(1);
		}

		[Test]
		public async Task TryOptimisticWriteAsync_should_fail_if_current_value_has_changed_externally()
		{
			// Use another sync store to change the blob value's ETag.
			// That should make the subject under test compare with
			// an old ETag, which should fail.
			var factory = Resolver.Resolve<IAzureStorageFactory>();
			var other = new BlobOptimisticSyncStore(factory);
			await other.InitializeAsync();
			var resultOfExternalModification = await other.TryOptimisticWriteAsync(2);
			resultOfExternalModification.ShouldBeTrue();

			var result = await _target.TryOptimisticWriteAsync(0);
			result.ShouldBeFalse();
		}
	}
}