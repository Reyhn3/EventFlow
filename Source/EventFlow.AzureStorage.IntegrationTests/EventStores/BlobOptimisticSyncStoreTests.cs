using System;
using System.Threading.Tasks;
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
		public async Task PreRun()
		{
			var target = Resolver.Resolve<IOptimisticSyncStore>();
			target.ShouldBeOfType<BlobOptimisticSyncStore>();
			_target = target as BlobOptimisticSyncStore;
		}

		[Test]
		public async Task InitializeAsync_should_create_container_and_blob_with_value_zero_if_it_does_not_exist()
		{
			await _target.InitializeAsync();
			var current = await _target.GetCurrentAsync();
			Console.WriteLine(current);
			current.ShouldBe(0);
		}

		[Test(Description = "Read the current value")]
		public async Task GetDataAsync_should_retrieve_the_current_value()
		{
			var result = await _target.GetCurrentAsync();
			Console.WriteLine(result);
		}

		[Test(Description = "Set a specific value")]
		public async Task TryOptimisticWriteAsync_should_write_new_value()
		{
			var result = await _target.TryOptimisticWriteAsync(0);
			result.ShouldBeTrue();
		}
	}
}