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
		
		public static IAsyncEnumerable<TResult> FullJoinAsync<TFirst, TSecond, TKey, TResult>(
			this IEnumerable<TFirst> first,
			IEnumerable<TSecond> second,
			Func<TFirst, TKey> firstKeySelector,
			Func<TSecond, TKey> secondKeySelector,
			Func<TFirst, TResult> firstSelector,
			Func<TSecond, Task<TResult>> secondSelector,
			Func<TFirst, TSecond, TResult> bothSelector,
			IEqualityComparer<TKey>? comparer)
		{
			if (first == null) throw new ArgumentNullException(nameof(first));
			if (second == null) throw new ArgumentNullException(nameof(second));
			if (firstKeySelector == null) throw new ArgumentNullException(nameof(firstKeySelector));
			if (secondKeySelector == null) throw new ArgumentNullException(nameof(secondKeySelector));
			if (firstSelector == null) throw new ArgumentNullException(nameof(firstSelector));
			if (secondSelector == null) throw new ArgumentNullException(nameof(secondSelector));
			if (bothSelector == null) throw new ArgumentNullException(nameof(bothSelector));

			return _();

			async IAsyncEnumerable<TResult> _()
			{
				var seconds = second.Select(e => new KeyValuePair<TKey, TSecond>(secondKeySelector(e), e)).ToArray();
				var secondLookup = seconds.ToLookup(e => e.Key, e => e.Value, comparer);
				var firstKeys = new HashSet<TKey>(comparer);

				foreach (var fe in first)
				{
					var key = firstKeySelector(fe);
					firstKeys.Add(key);

					using var se = secondLookup[key].GetEnumerator();

					if (se.MoveNext())
					{
						do
						{
							yield return bothSelector(fe, se.Current);
						} while (se.MoveNext());
					}
					else
					{
						se.Dispose();
						yield return firstSelector(fe);
					}
				}

				foreach (var (key, value) in seconds)
					if (!firstKeys.Contains(key))
						yield return await secondSelector(value);
			}
		}
	}
}