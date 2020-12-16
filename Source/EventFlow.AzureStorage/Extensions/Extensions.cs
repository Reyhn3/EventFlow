using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;


namespace EventFlow.AzureStorage.Extensions
{
	internal static class Extensions
	{
		public static IEnumerable<T> AsEnumerable<T>(this T element)
		{
			return element == null ? Enumerable.Empty<T>() : new[] {element};
		}

		public static async IAsyncEnumerable<TResult> LeftJoinAsync<TFirst, TSecond, TKey, TResult>(
			this IEnumerable<TFirst> first,
			IEnumerable<TSecond> second,
			Func<TFirst, TKey> firstKeySelector,
			Func<TSecond, TKey> secondKeySelector,
			Func<TFirst, Task<TResult>> firstSelector,
			Func<TFirst, TSecond, Task<TResult>> bothSelector,
			IEqualityComparer<TKey>? comparer)
		{
			if (first == null) throw new ArgumentNullException(nameof(first));
			if (second == null) throw new ArgumentNullException(nameof(second));
			if (firstKeySelector == null) throw new ArgumentNullException(nameof(firstKeySelector));
			if (secondKeySelector == null) throw new ArgumentNullException(nameof(secondKeySelector));
			if (firstSelector == null) throw new ArgumentNullException(nameof(firstSelector));
			if (bothSelector == null) throw new ArgumentNullException(nameof(bothSelector));

			var groups = first
				.GroupJoin(
					second,
					firstKeySelector,
					secondKeySelector,
					(f, s) => (
						Value: f,
						Seconds: s.Select(ss => (
							HasValue: true,
							Value: ss))),
					comparer);

			foreach (var (f, seconds) in groups)
			foreach (var (secondHasValue, s) in seconds.DefaultIfEmpty())
				if (secondHasValue)
					yield return await bothSelector(f, s).ConfigureAwait(false);
				else
					yield return await firstSelector(f).ConfigureAwait(false);
		}
	}
}