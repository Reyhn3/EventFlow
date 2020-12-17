using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;


namespace EventFlow.AzureStorage.Extensions
{
	internal static class Extensions
	{
		public static IEnumerable<T> AsEnumerable<T>(this T element)
		{
			return element == null ? Enumerable.Empty<T>() : new[] {element};
		}

		public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> sequence, int batchSize, bool excludeNull = false)
		{
			if (batchSize <= 0)
				throw new ArgumentOutOfRangeException(nameof(batchSize), batchSize, "Batch size must be a positive non-zero number");
				
			return sequence
				.Where(e => excludeNull ? e != null : true)
				.Select((Value, Index) => new {Index, Value})
				.GroupBy(x => x.Index / batchSize)
				.Select(g => g.Select(x => x.Value));
		}

		public static IAsyncEnumerable<IAsyncEnumerable<T>> Batch<T>(this IAsyncEnumerable<T> asyncSequence, int batchSize, bool excludeNull = false)
		{
			if (batchSize <= 0)
				throw new ArgumentOutOfRangeException(nameof(batchSize), batchSize, "Batch size must be a positive non-zero number");
				
			return asyncSequence
				.Where(e => excludeNull ? e != null : true)
				.Select((Value, Index) => new {Index, Value})
				.GroupBy(x => x.Index / batchSize)
				.Select(g => g.Select(x => x.Value));
		}

		/// <summary>
		///     Applies the given <paramref name="action" /> to each element in the <paramref name="asyncEnumerable" />
		///     and yields the element if the <paramref name="action" /> returns <c>true</c>; otherwise the element is skipped.
		/// </summary>
		/// <typeparam name="T">The type of elements</typeparam>
		/// <param name="asyncEnumerable">The sequence of elements to iterate</param>
		/// <param name="action">The action to apply to each element</param>
		/// <param name="cancellationToken">Cancellation token</param>
		/// <returns></returns>
		public static async IAsyncEnumerable<T> ApplyOrExcludeAsync<T>(
			this IAsyncEnumerable<T> asyncEnumerable,
			Func<T, Task<bool>> action,
			[EnumeratorCancellation] CancellationToken cancellationToken)
		{
			await foreach (var element in asyncEnumerable.WithCancellation(cancellationToken).ConfigureAwait(false))
			{
				var isSuccessful = await action(element).ConfigureAwait(false);
				if (isSuccessful)
					yield return element;
			}
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