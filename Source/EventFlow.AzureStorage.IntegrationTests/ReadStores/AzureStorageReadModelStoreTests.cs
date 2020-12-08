using System.Threading;
using System.Threading.Tasks;
using EventFlow.AzureStorage.Config;
using EventFlow.AzureStorage.Connection;
using EventFlow.AzureStorage.ReadStores;
using EventFlow.Logs;
using EventFlow.ReadStores;
using EventFlow.TestHelpers;
using FakeItEasy;
using NUnit.Framework;
using Shouldly;


namespace EventFlow.AzureStorage.IntegrationTests.ReadStores
{
	[Explicit("Intended for manual verification")]
	[Category(Categories.Integration)]
	public class AzureStorageReadModelStoreTests
	{
		private AzureStorageReadModelStore<DummyReadModel> _target;

		[SetUp]
		public async Task PreRun()
		{
			var config = new AzureStorageConfiguration
				{
					StorageAccountConnectionString = "UseDevelopmentStorage=true",
					ReadStoreTableName = "EventFlowReadModels"
				};

			var factory = new AzureStorageFactory(config);
			await factory.InitializeAsync().ConfigureAwait(false);
			_target = new AzureStorageReadModelStore<DummyReadModel>(A.Dummy<ILog>(), factory);
		}

		[Test]
		public async Task UpdateAsync_should_insert_the_read_model_if_it_is_new()
		{
			Assert.Fail("Test not implemented");
		}

		[Test]
		public async Task UpdateAsync_should_update_the_read_model_if_it_already_exists()
		{
			Assert.Fail("Test not implemented");
		}

		[Test]
		public async Task UpdateAsync_should_delete_the_read_model_if_it_is_marked_for_deletion()
		{
			Assert.Fail("Test not implemented");
		}

		[Test]
		public async Task GetAsync_should_retrieve_the_specified_read_model()
		{
			var result = await _target.GetAsync("test", CancellationToken.None);
			result.ReadModel.ShouldNotBeNull();
		}

		[Test]
		public async Task DeleteAsync_should_delete_the_specified_read_model()
		{
			Assert.Fail("Test not implemented");
		}

		[Test]
		public async Task DeleteAllAsync_should_delete_all_read_models_of_the_specified_type()
		{
			Assert.Fail("Test not implemented");
		}


		private class DummyReadModel : IReadModel
		{}
	}
}