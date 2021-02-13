using System;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.Aggregates;
using EventFlow.AzureStorage.Config;
using EventFlow.AzureStorage.EventStores;
using EventFlow.AzureStorage.IntegrationTests.Domain;
using EventFlow.Core;
using EventFlow.EventStores;
using EventFlow.Extensions;
using FakeItEasy;
using NUnit.Framework;
using Shouldly;


namespace EventFlow.AzureStorage.IntegrationTests.EventStores
{
	[Explicit("Intended for manual verification")]
	public class AzureStorageEventPersistenceTests : IntegrationTests
	{
		private AzureStorageEventPersistence _target;

		public AzureStorageEventPersistenceTests()
			: base(o =>
				{
					o.UseAzureStorageEventStore();
					o.UseInMemoryReadStoreFor<FundReadModel>();
				})
		{}

		[SetUp]
		public void PreRun()
		{
			var target = Resolver.Resolve<IEventPersistence>();
			target.ShouldBeOfType<AzureStorageEventPersistence>();
			_target = target as AzureStorageEventPersistence;
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