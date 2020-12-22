using System;
using System.Linq;
using System.Threading.Tasks;
using EventFlow.AzureStorage.Config;
using EventFlow.AzureStorage.EventStores;
using EventFlow.TestHelpers;
using FakeItEasy;
using NUnit.Framework;
using Shouldly;


namespace EventFlow.AzureStorage.Tests.EventStores
{
	[Category(Categories.Unit)]
	public class UniqueIdGeneratorTests
	{
		private const int RangeSize = 5;

		private IOptimisticSyncStore _store;
		private UniqueIdGenerator _target;

		[SetUp]
		public void PreRun()
		{
			_store = A.Fake<IOptimisticSyncStore>(f => f.ConfigureFake(ff =>
				{
					var currentNumbersFromStore = Enumerable.Range(0, 5).Select(i => i * 5L).ToArray();
					A.CallTo(() => ff.GetCurrentAsync())
						.ReturnsNextFromSequence(currentNumbersFromStore);

					A.CallTo(() => ff.TryOptimisticWriteAsync(A<long>.Ignored))
						.Returns(Task.FromResult(true));
				}));

			var config = A.Fake<AzureStorageConfiguration>(f => f.ConfigureFake(ff => 
				A.CallTo(() => ff.SequenceNumberRangeSize)
					.Returns(RangeSize)));
			_target = new UniqueIdGenerator(config, _store);
		}

		[Test]
		public async Task GetNextIdAsync_should_return_the_next_sequential_number()
		{
			var result = await _target.GetNextIdAsync();
			result.ShouldBe(1);
		}

		[Test]
		public async Task GetNextIdAsync_should_initialize_once()
		{
			var result1 = await _target.GetNextIdAsync();
			result1.ShouldBe(1);

			var result2 = await _target.GetNextIdAsync();
			result2.ShouldBe(2);

			A.CallTo(() => _store.GetCurrentAsync()).MustHaveHappenedOnceExactly();
		}

		[Test]
		public async Task GetNextIdAsync_should_get_new_range_from_store_when_current_range_is_exhausted()
		{
			for (var i = 0; i < RangeSize; i++)
			{
				var result0 = await _target.GetNextIdAsync();
				Console.WriteLine(result0);
			}

			var result = await _target.GetNextIdAsync();
			Console.WriteLine(result);
			result.ShouldBe(RangeSize + 1);

			A.CallTo(() => _store.GetCurrentAsync()).MustHaveHappenedTwiceExactly();
		}

		[Test]
		public async Task GetNextIdAsync_should_use_the_next_available_number_provided_by_the_store()
		{
			for (var i = 0; i < RangeSize; i++)
			{
				var result0 = await _target.GetNextIdAsync();
				Console.WriteLine(result0);
			}

			const long nextAvailableFromStore = 27L;
			A.CallTo(() => _store.GetCurrentAsync())
				.Returns(Task.FromResult(nextAvailableFromStore));

			var result = await _target.GetNextIdAsync();
			Console.WriteLine(result);
			result.ShouldBe(nextAvailableFromStore + 1);

			A.CallTo(() => _store.GetCurrentAsync()).MustHaveHappenedTwiceExactly();
		}
	}
}