using System;
using System.Collections.Generic;
using System.Linq;
using EventFlow.AzureStorage.Connection;
using EventFlow.AzureStorage.ReadStores;
using EventFlow.Logs;
using EventFlow.ReadStores;
using EventFlow.TestHelpers;
using FakeItEasy;
using NUnit.Framework;
using Shouldly;


namespace EventFlow.AzureStorage.Tests.ReadStores
{
	[Category(Categories.Unit)]
	public class AzureStorageReadModelStoreTests
	{
		private AzureStorageReadModelStore<DummyReadModel> _target;

		[SetUp]
		public void PreRun()
		{
			_target = new AzureStorageReadModelStore<DummyReadModel>(
				A.Fake<ILog>(),
				A.Fake<IAzureStorageFactory>(),
				A.Fake<IReadModelFactory<DummyReadModel>>());
		}

		[Test]
		public void GroupByRunningLength_should_fill_groups_to_max_size_or_smaller_unless_element_is_oversized()
		{
			var readModelIds = CreateReadModelIds(
					2, // Group 0
					3, // Group 0
					1, // Group 0 - Group is perfectly filled by now
					3, // Group 1
					2, // Group 1
					5, // Group 2 - Doesn't fit in the previous group
					2, // Group 3 - Doesn't fit in the previous group
					7, // Group 4 - Element exceeds the max length, so include it in its own group, but don't affect the following group
					2, // Group 5
					1, // Group 5
					3, // Group 5 - Group is perfectly filled by now, and is unaffected by the length of group 4
					1, //´Group 6
					6, // Group 7 - Doesn't fit in the previous group
					3, // Group 8
					3, // Group 8
					2  // Group 9 - Still has some room left
				);


			var result = _target.GroupByRunningLength(readModelIds, CalculateQueryFilterMaxLength, CalculateRecordFilterLength).ToArray();


			result.ShouldNotBeNull();

			foreach (var grouping in result)
			{
				Console.WriteLine(grouping.Key);
				foreach (var element in grouping)
					Console.WriteLine("\t{0}", element);
			}

			result.Length.ShouldBe(10);
			result[0].Count().ShouldBe(3);
			result[1].Count().ShouldBe(2);
			result[2].Count().ShouldBe(1);
			result[3].Count().ShouldBe(1);
			result[4].Count().ShouldBe(1);
			result[5].Count().ShouldBe(3);
			result[6].Count().ShouldBe(1);
			result[7].Count().ShouldBe(1);
			result[8].Count().ShouldBe(2);
			result[9].Count().ShouldBe(1);
		}

		[Test]
		public void GroupByRunningLength_should_place_elements_that_fit_inside_the_same_group()
		{
			var readModelIds = CreateReadModelIds(2, 2, 2);


			var result = _target.GroupByRunningLength(readModelIds, CalculateQueryFilterMaxLength, CalculateRecordFilterLength).ToArray();


			result.ShouldNotBeNull();
			PrintGroupings(result);
			result.Length.ShouldBe(1);
			result[0].Count().ShouldBe(3);
		}

		[Test]
		public void GroupByRunningLength_should_place_the_element_that_overflows_in_the_next_group_when_undersized()
		{
			var readModelIds = CreateReadModelIds(5, 2);


			var result = _target.GroupByRunningLength(readModelIds, CalculateQueryFilterMaxLength, CalculateRecordFilterLength).ToArray();


			result.ShouldNotBeNull();
			PrintGroupings(result);
			result.Length.ShouldBe(2);
			result[0].Count().ShouldBe(1);
			result[1].Count().ShouldBe(1);
		}

		[Test]
		public void GroupByRunningLength_should_place_the_element_that_overflows_in_the_next_group_when_oversized()
		{
			var readModelIds = CreateReadModelIds(2, 2, 3);


			var result = _target.GroupByRunningLength(readModelIds, CalculateQueryFilterMaxLength, CalculateRecordFilterLength).ToArray();


			result.ShouldNotBeNull();
			PrintGroupings(result);
			result.Length.ShouldBe(2);
			result[0].Count().ShouldBe(2);
			result[1].Count().ShouldBe(1);
		}

		[Test]
		public void GroupByRunningLength_should_place_the_element_that_overflows_in_the_next_group_when_current_group_is_not_full()
		{
			var readModelIds = CreateReadModelIds(2, 7);


			var result = _target.GroupByRunningLength(readModelIds, CalculateQueryFilterMaxLength, CalculateRecordFilterLength).ToArray();


			result.ShouldNotBeNull();
			PrintGroupings(result);
			result.Length.ShouldBe(2);
			result[0].Count().ShouldBe(1);
			result[1].Count().ShouldBe(1);
		}

		[Test]
		public void
			GroupByRunningLength_should_place_the_element_that_overflows_in_the_next_group_when_current_group_is_not_full_and_not_affect_subsequent_groups()
		{
			var readModelIds = CreateReadModelIds(2, 7, 2, 1, 3, 1);


			var result = _target.GroupByRunningLength(readModelIds, CalculateQueryFilterMaxLength, CalculateRecordFilterLength).ToArray();


			result.ShouldNotBeNull();
			PrintGroupings(result);
			result.Length.ShouldBe(4);
			result[0].Count().ShouldBe(1);
			result[1].Count().ShouldBe(1);
			result[2].Count().ShouldBe(3);
			result[3].Count().ShouldBe(1);
		}

		[Test]
		public void
			GroupByRunningLength_should_place_the_element_that_overflows_in_the_next_group_when_current_group_is_not_full_and_not_affect_subsequent_groups_with_oversized_elements()
		{
			var readModelIds = CreateReadModelIds(2, 7, 7, 1, 3, 2);


			var result = _target.GroupByRunningLength(readModelIds, CalculateQueryFilterMaxLength, CalculateRecordFilterLength).ToArray();


			result.ShouldNotBeNull();
			PrintGroupings(result);
			result.Length.ShouldBe(4);
			result[0].Count().ShouldBe(1);
			result[1].Count().ShouldBe(1);
			result[2].Count().ShouldBe(1);
			result[3].Count().ShouldBe(3);
		}

		[Test]
		public void GroupByRunningLength_should_not_affect_subsequent_groups()
		{
			var readModelIds = CreateReadModelIds(
				7,  // Group 0 - Element exceeds the max length, so include it in its own group, but don't affect the following group
				3,  // Group 1
				4); // Group 2 - If this fits in group 1, it means group 0 forwarded a negative reminder to group 1


			var result = _target.GroupByRunningLength(readModelIds, CalculateQueryFilterMaxLength, CalculateRecordFilterLength).ToArray();


			result.ShouldNotBeNull();
			PrintGroupings(result);
			result.Length.ShouldBe(3);
			result[0].Key.ShouldBe(0);
			result[1].Key.ShouldBe(1);
			result[2].Key.ShouldBe(2);
			result[0].Count().ShouldBe(1);
			result[1].Count().ShouldBe(1);
			result[2].Count().ShouldBe(1);
		}

		[Test]
		public void GroupByRunningLength_should_limit_the_number_of_elements_per_group()
		{
			// The number of elements in one group is limited,
			// even though they fit. Create one more element
			// than permitted, to assert the last element ends
			// up in its own group.
			var elements = Enumerable.Repeat(1, 222).ToArray();
			var readModelIds = CreateReadModelIds(elements);


			var result = _target.GroupByRunningLength(readModelIds, () => 1000, id => 1).ToArray();


			result.ShouldNotBeNull();
			PrintGroupings(result);
			result.Length.ShouldBe(3);
			result[0].Count().ShouldBe(110);
			result[1].Count().ShouldBe(110);
			result[2].Count().ShouldBe(2);
		}
	
		private static int CalculateQueryFilterMaxLength() => 6;
		private static int CalculateRecordFilterLength(string readModelId) => readModelId.Length;

		private static IEnumerable<string> CreateReadModelIds(params int[] lengths) =>
			lengths
				.Select((length, e) => ((char)('a' + (e % 'a') % ('z' - 'a' + 1)), length))
				.Select(e => new string(e.Item1, e.length));

		private static void PrintGroupings(IEnumerable<IGrouping<int, string>> result)
		{
			foreach (var grouping in result)
			{
				Console.WriteLine(grouping.Key);
				foreach (var element in grouping)
					Console.WriteLine("\t{0}", element);
			}
		}


		public class DummyReadModel : IReadModel
		{}
	}
}