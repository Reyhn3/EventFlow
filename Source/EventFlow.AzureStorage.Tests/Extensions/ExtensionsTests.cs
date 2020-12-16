using System.Linq;
using System.Threading.Tasks;
using EventFlow.AzureStorage.Extensions;
using EventFlow.TestHelpers;
using NUnit.Framework;
using Shouldly;


namespace EventFlow.AzureStorage.Tests.Extensions
{
	[Category(Categories.Unit)]
	public class ExtensionsTests
	{
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