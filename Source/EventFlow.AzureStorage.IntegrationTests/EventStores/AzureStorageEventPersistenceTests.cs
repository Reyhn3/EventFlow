using System;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.Aggregates;
using EventFlow.AzureStorage.Config;
using EventFlow.AzureStorage.Connection;
using EventFlow.AzureStorage.EventStores;
using EventFlow.Core;
using EventFlow.EventStores;
using EventFlow.Logs;
using EventFlow.TestHelpers;
using FakeItEasy;
using NUnit.Framework;


namespace EventFlow.AzureStorage.IntegrationTests.EventStores
{
	[Explicit("Intended for manual verification")]
	[Category(Categories.Integration)]
	public class AzureStorageEventPersistenceTests
	{
		private AzureStorageEventPersistence _target;

		[SetUp]
		public async Task PreRun()
		{
			var config = new AzureStorageConfiguration
				{
					StorageAccountConnectionString = "UseDevelopmentStorage=true",
					EventStoreTableName = "EventFlowEventsTEST"
				};
			var azureStorageFactory = new AzureStorageFactory(config);
			var optimisticSyncStore = new BlobOptimisticSyncStore(azureStorageFactory);
			var bootstrapper = new AzureStorageBootstrap(azureStorageFactory, optimisticSyncStore);
			var uniqueIdGenerator = new UniqueIdGenerator(config, optimisticSyncStore);
			_target = new AzureStorageEventPersistence(A.Dummy<ILog>(), azureStorageFactory, uniqueIdGenerator);
			await bootstrapper.BootAsync(CancellationToken.None);
		}

		[Test]
		public async Task CommitEventsAsync_should_insert_single_event()
		{
			var id = A.Fake<IIdentity>(f => f.ConfigureFake(ff =>
				A.CallTo(() => ff.Value)
					.Returns("test-id-a")));

			var metadata = A.Fake<IMetadata>(f => f.ConfigureFake(ff =>
				A.CallTo(() => ff[MetadataKeys.BatchId])
					.Returns(Guid.Empty.ToString())));
			var events = new[]
				{
					new SerializedEvent("metadata", "data-event-0", 0, metadata)
				};


			await _target.CommitEventsAsync(id, events, CancellationToken.None);
		}

		[Test]
		public async Task CommitEventsAsync_should_insert_multiple_events()
		{
			var id = A.Fake<IIdentity>(f => f.ConfigureFake(ff =>
				A.CallTo(() => ff.Value)
					.Returns("test-id-b")));

			var metadata = A.Fake<IMetadata>(f => f.ConfigureFake(ff =>
				A.CallTo(() => ff[MetadataKeys.BatchId])
					.Returns(Guid.Empty.ToString())));
			
			var events = new[]
				{
					new SerializedEvent("metadata", "data-event-0", 0, metadata),
					new SerializedEvent("metadata", "data-event-1", 1, metadata)
				};


			await _target.CommitEventsAsync(id, events, CancellationToken.None);
		}
	}
}