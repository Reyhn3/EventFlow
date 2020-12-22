using System.Linq;
using System.Threading.Tasks;
using EventFlow.TestHelpers;
using NUnit.Framework;
using Shouldly;


namespace EventFlow.AzureStorage.Tests.Extensions
{
	[Category(Categories.Unit)]
	public class ExtensionsTests
	{
		[Test]
		public void Batch_should_create_groups_of_the_specified_size()
		{
			const string input = "abcdefghij";
			
			
			var result = input.Batch(3).ToArray();
			
			
			result.Length.ShouldBe(4);
			result[0].Count().ShouldBe(3);
			result[1].Count().ShouldBe(3);
			result[2].Count().ShouldBe(3);
			result[3].Count().ShouldBe(1);
		}
		
		[Test]
		public void Batch_should_exclude_null()
		{
			var input = new string[]
				{
					"a",
					"b",
					null,
					"d",
					"e",
					"f",
					null,
					null,
					"i",
					"j",
				};
			
			
			var result = input.Batch(3, true).ToArray();
			
			
			result.Length.ShouldBe(3);
			result[0].Count().ShouldBe(3);
			result[1].Count().ShouldBe(3);
			result[2].Count().ShouldBe(1);
		}
		
		[Test]
		public async Task LeftJoinAsync_should_include_all_elements_in_left_sequence_and_exclude_extra_elements_from_right()
		{
			const string left = "abcdef";
			const string right = "defghi";


			var result = left.LeftJoinAsync(right, l => l, r => r, l => Task.FromResult(l), (l, r) => Task.FromResult(l), null);
			var resultList = await result.ToArrayAsync();


			new string(resultList).ShouldBe("abcdef");
		}
	}
}