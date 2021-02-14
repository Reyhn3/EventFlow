using System;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.AzureStorage.Config;


namespace EventFlow.AzureStorage.EventStores
{
	/// <summary>
	///     Used to generate simple, unique identifiers across multiple environments, processes and/or threads. Requires a
	///     global data store that can be used to store the last upper limit (must implement <see cref="IOptimisticSyncStore"/>).
	///     Contention is reduced by allocating ranges to each instance of the <see cref="UniqueIdGenerator"/>.
	/// </summary>
	public class UniqueIdGenerator : IUniqueIdGenerator
	{
		private static readonly SemaphoreSlim Lock = new SemaphoreSlim(1);

		private readonly IOptimisticSyncStore _optimisticSyncStore;
		private readonly int _rangeSize;
		private readonly int _maxRetries;

		private long _lastId;
		private long _upperLimit;

		public UniqueIdGenerator(AzureStorageConfiguration configuration, IOptimisticSyncStore optimisticSyncStore)
		{
			_optimisticSyncStore = optimisticSyncStore;
			_rangeSize = configuration.SequenceNumberRangeSize;
			_maxRetries = configuration.SequenceNumberOptimisticConcurrencyRetries;
		}

		/// <summary>
		///     Fetches the next available unique ID.
		/// </summary>
		public async Task<long> GetNextIdAsync()
		{
			await Lock.WaitAsync().ConfigureAwait(false);
			try
			{
				if (_lastId == _upperLimit)
					await UpdateFromSyncStoreAsync().ConfigureAwait(false);

				return Interlocked.Increment(ref _lastId);
			}
			finally
			{
				Lock.Release();
			}
		}

		private async Task UpdateFromSyncStoreAsync()
		{
			var retryCount = 0;

			// maxRetries + 1 because the first run isn't a "re"try.
			while (retryCount < _maxRetries + 1)
			{
				_lastId = await _optimisticSyncStore.GetCurrentAsync().ConfigureAwait(false);
				_upperLimit = _lastId + _rangeSize;

				if (await _optimisticSyncStore.TryOptimisticWriteAsync(_upperLimit).ConfigureAwait(false))
					return;

				retryCount++;
			}

			throw new Exception($"Failed to update the OptimisticSyncStore after {retryCount} attempts");
		}
	}
}