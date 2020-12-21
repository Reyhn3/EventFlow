using System;
using EventFlow.Aggregates;
using EventFlow.AzureStorage.Connection;
using EventFlow.AzureStorage.SnapshotStores;
using EventFlow.Core;
using EventFlow.Logs;
using EventFlow.TestHelpers;
using FakeItEasy;
using NUnit.Framework;
using Shouldly;


namespace EventFlow.AzureStorage.Tests.SnapshotStores
{
	[Category(Categories.Unit)]
	public class AzureStorageSnapshotPersistenceTests
	{
		private AzureStorageSnapshotPersistence _target;

		[SetUp]
		public void PreRun()
		{
			_target = new AzureStorageSnapshotPersistence(A.Dummy<ILog>(), A.Dummy<IAzureStorageFactory>());
		}

		[Test]
		public void GetPartitionKey_should_combine_aggregate_type_with_aggregate_id()
		{
			var aggregate = A.Fake<IAggregateRoot>(f => f.WithAttributes(() => new AggregateNameAttribute("DummyAggregate")));
			var identity = A.Fake<IIdentity>(f => f.ConfigureFake(ff => A.CallTo(() => ff.Value).Returns("123")));


			var result = AzureStorageSnapshotPersistence.GetPartitionKey(aggregate.GetType(), identity);


			Console.WriteLine(result);
			result.ShouldNotBeNull();
			result.ShouldBe("DummyAggregate::123");
		}

		[Test]
		public void GetPartitionKey_should_allow_null_as_identity()
		{
			var aggregate = A.Fake<IAggregateRoot>(f => f.WithAttributes(() => new AggregateNameAttribute("DummyAggregate")));


			var result = AzureStorageSnapshotPersistence.GetPartitionKey(aggregate.GetType(), null);


			Console.WriteLine(result);
			result.ShouldNotBeNull();
			result.ShouldBe("DummyAggregate::");
		}

		[Test(Description = "The int should be reversed by subtracting it from int.MaxValue, so the lowest number becomes the highest number etc")]
		[TestCase(0, "2147483647")]
		[TestCase(1, "2147483646")]
		[TestCase(int.MaxValue, "0000000000")]
		public void GetRowKey_should_reverse_the_sequence_number(int aggregateSequenceNumber, string expected)
		{
			AzureStorageSnapshotPersistence.GetRowKey(aggregateSequenceNumber).ShouldBe(expected);
		}
	}
}