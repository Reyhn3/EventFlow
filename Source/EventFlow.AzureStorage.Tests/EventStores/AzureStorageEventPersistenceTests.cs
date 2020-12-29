using System;
using EventFlow.AzureStorage.EventStores;
using EventFlow.Core;
using EventFlow.TestHelpers;
using FakeItEasy;
using NUnit.Framework;
using Shouldly;


namespace EventFlow.AzureStorage.Tests.EventStores
{
	[Category(Categories.Unit)]
	public class AzureStorageEventPersistenceTests
	{
		[Test]
		public void GetPartitionKey_should_combine_aggregate_type_with_aggregate_id()
		{
			var identity = A.Fake<IIdentity>(f => f.ConfigureFake(ff =>
				A.CallTo(() => ff.Value)
					.Returns("123")));


			var result = AzureStorageEventPersistence.GetPartitionKey("DummyAggregate", identity);


			Console.WriteLine(result);
			result.ShouldNotBeNull();
			result.ShouldBe("DummyAggregate::123");
		}

		[Test]
		public void GetPartitionKey_should_allow_null_as_identity()
		{
			var result = AzureStorageEventPersistence.GetPartitionKey("DummyAggregate", null);
			Console.WriteLine(result);
			result.ShouldNotBeNull();
			result.ShouldBe("DummyAggregate::");
		}

		[Test]
		public void GetRowKey_should_left_pad_the_sequence_number_with_zeros()
		{
			AzureStorageEventPersistence.GetRowKey(1).ShouldBe("0000000001");
		}
	}
}