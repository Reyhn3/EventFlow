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
			// Arrange

			var id = A.Fake<IIdentity>(f => f.ConfigureFake(ff =>
				A.CallTo(() => ff.Value)
					.Returns("test-id-commit-single")));

			var metadata = A.Fake<IMetadata>(f => f.ConfigureFake(ff =>
				A.CallTo(() => ff[MetadataKeys.BatchId])
					.Returns(Guid.Empty.ToString())));
			var events = new[]
				{
					new SerializedEvent("metadata", "data-event-0", 0, metadata)
				};


			// Act

			await _target.CommitEventsAsync(id, events, CancellationToken.None);


			// Assert

			var result = await _target.LoadCommittedEventsAsync(id, 0, A.Dummy<CancellationToken>());
			result.ShouldNotBeEmpty();
			result.Count.ShouldBe(1);
		}

		[Test]
		public async Task CommitEventsAsync_should_insert_multiple_events()
		{
			// Arrange

			var id = A.Fake<IIdentity>(f => f.ConfigureFake(ff =>
				A.CallTo(() => ff.Value)
					.Returns("test-id-commit-multiple")));

			var metadata = A.Fake<IMetadata>(f => f.ConfigureFake(ff =>
				A.CallTo(() => ff[MetadataKeys.BatchId])
					.Returns(Guid.Empty.ToString())));

			var events = new[]
				{
					new SerializedEvent("metadata", "data-event-0", 0, metadata),
					new SerializedEvent("metadata", "data-event-1", 1, metadata)
				};


			// Act

			await _target.CommitEventsAsync(id, events, CancellationToken.None);


			// Assert

			var result = await _target.LoadCommittedEventsAsync(id, 0, A.Dummy<CancellationToken>());
			result.ShouldNotBeEmpty();
			result.Count.ShouldBe(2);
		}

		[Test]
		public async Task LoadCommittedEventsAsync_should_load_all_events_committed_for_the_specified_aggregate()
		{
			// Arrange

			var id = A.Fake<IIdentity>(f => f.ConfigureFake(ff =>
				A.CallTo(() => ff.Value)
					.Returns("test-id-load")));

			var metadata = A.Fake<IMetadata>(f => f.ConfigureFake(ff =>
				A.CallTo(() => ff[MetadataKeys.BatchId])
					.Returns(Guid.Empty.ToString())));

			var events = new[]
				{
					new SerializedEvent("metadata", "data-event-0", 0, metadata),
					new SerializedEvent("metadata", "data-event-1", 1, metadata)
				};

			await _target.CommitEventsAsync(id, events, CancellationToken.None);


			// Act

			var result = await _target.LoadCommittedEventsAsync(id, 0, A.Dummy<CancellationToken>());


			// Assert

			result.ShouldNotBeEmpty();
			result.Count.ShouldBe(2);
		}

		[Order(0)]
		[Test]
		public async Task LoadAllCommittedEvents_should_load_all_events_committed_for_all_aggregates()
		{
			// Arrange

			var id1 = A.Fake<IIdentity>(f => f.ConfigureFake(ff =>
				A.CallTo(() => ff.Value)
					.Returns("test-id-loadall-a")));
			var id2 = A.Fake<IIdentity>(f => f.ConfigureFake(ff =>
				A.CallTo(() => ff.Value)
					.Returns("test-id-loadall-b")));

			var metadata = A.Fake<IMetadata>(f => f.ConfigureFake(ff =>
				A.CallTo(() => ff[MetadataKeys.BatchId])
					.Returns(Guid.Empty.ToString())));

			var events = new[]
				{
					new SerializedEvent("metadata", "data-event-0", 0, metadata)
				};

			await _target.CommitEventsAsync(id1, events, CancellationToken.None);
			await _target.CommitEventsAsync(id2, events, CancellationToken.None);


			// Act

			var result = await _target.LoadAllCommittedEvents(GlobalPosition.Start, 100, A.Dummy<CancellationToken>());


			// Assert

			result.ShouldNotBeNull();
			result.CommittedDomainEvents.ShouldNotBeEmpty();
			result.CommittedDomainEvents.Count.ShouldBe(2);
		}

		[Test]
		public async Task DeleteEventsAsync_should_delete_all_events_for_the_specified_aggregate()
		{
			// Arrange

			var id1 = A.Fake<IIdentity>(f => f.ConfigureFake(ff =>
				A.CallTo(() => ff.Value)
					.Returns("test-id-delete-a")));
			var id2 = A.Fake<IIdentity>(f => f.ConfigureFake(ff =>
				A.CallTo(() => ff.Value)
					.Returns("test-id-delete-b")));

			var metadata = A.Fake<IMetadata>(f => f.ConfigureFake(ff =>
				A.CallTo(() => ff[MetadataKeys.BatchId])
					.Returns(Guid.Empty.ToString())));

			var events = new[]
				{
					new SerializedEvent("metadata", "data-event-0", 0, metadata)
				};

			await _target.CommitEventsAsync(id1, events, CancellationToken.None);
			await _target.CommitEventsAsync(id2, events, CancellationToken.None);


			// Act

			await _target.DeleteEventsAsync(id1, A.Dummy<CancellationToken>());


			// Assert

			var result1 = await _target.LoadCommittedEventsAsync(id1, 0, A.Dummy<CancellationToken>());
			var result2 = await _target.LoadCommittedEventsAsync(id2, 0, A.Dummy<CancellationToken>());
			result1.ShouldBeEmpty();
			result2.ShouldNotBeEmpty();
		}
	}
}