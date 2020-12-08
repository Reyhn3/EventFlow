using System;
using System.Threading.Tasks;
using EventFlow.AzureStorage.Config;
using EventFlow.AzureStorage.Connection;
using EventFlow.AzureStorage.EventStores;
using EventFlow.TestHelpers;
using NUnit.Framework;
using Shouldly;


namespace EventFlow.AzureStorage.IntegrationTests.EventStores
{
	[Explicit("Integration tests")]
	[Category(Categories.Integration)]
	public class BlobOptimisticSyncStoreTests
	{
		private BlobOptimisticSyncStore _target;

		[SetUp]
		public async Task PreRun()
		{
			var config = new AzureStorageConfiguration
				{
					StorageAccountConnectionString = "UseDevelopmentStorage=true",
					SystemContainerName = "eventflow-system-params",
					EventStoreTableName = "EventFlowEvents"
				};
			var factory = new AzureStorageFactory(config);
			await factory.InitializeAsync().ConfigureAwait(false);
			_target = new BlobOptimisticSyncStore(factory);
			await _target.InitializeAsync().ConfigureAwait(false);
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