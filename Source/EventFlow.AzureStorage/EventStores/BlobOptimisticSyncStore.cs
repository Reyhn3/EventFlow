using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs.Models;
using EventFlow.AzureStorage.Connection;


namespace EventFlow.AzureStorage.EventStores
{
	/// <summary>
	///     Stores a single string value in Blob storage and provides an easy way to update
	///     the value using optimistic concurrency.
	/// </summary>
	public class BlobOptimisticSyncStore : IOptimisticSyncStore
	{
		private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1);
		private readonly IAzureStorageFactory _factory;
		
		private bool _isInitialized;
		private ETag _etag;

		public BlobOptimisticSyncStore(IAzureStorageFactory factory)
		{
			_factory = factory ?? throw new ArgumentNullException(nameof(factory));
		}

		public async Task InitializeAsync()
		{
			await _semaphore.WaitAsync().ConfigureAwait(false);

			try
			{
				if (_isInitialized)
					return;

				var blob = _factory.CreateBlobClientForSequenceNumber();
				if (!await blob.ExistsAsync().ConfigureAwait(false))
					await blob.UploadAsync(new MemoryStream(BitConverter.GetBytes(0L))).ConfigureAwait(false);

				var properties = await blob.GetPropertiesAsync().ConfigureAwait(false);
				_etag = properties.Value.ETag;

				_isInitialized = true;
			}
			finally
			{
				_semaphore.Release();
			}
		}

		public async Task<long> GetCurrentAsync()
		{
			var blob = _factory.CreateBlobClientForSequenceNumber();
			var download = await blob.DownloadAsync().ConfigureAwait(false);
			var buffer = await ReadBytesFromStreamAsync(download.Value.Content, sizeof(long)).ConfigureAwait(false);
			if (buffer.Length == 0)
				throw new InvalidDataException("The global sequence blob existed but was empty");

			var data = BitConverter.ToInt64(buffer);
			return data;
		}

		public async Task<bool> TryOptimisticWriteAsync(long data)
		{
			try
			{
				var options = new BlobUploadOptions
					{
						Conditions = new BlobRequestConditions
							{
								IfMatch = _etag
							}
					};

				var blob = _factory.CreateBlobClientForSequenceNumber();
				await using var stream = await WriteDataToStreamAsync(data).ConfigureAwait(false);
				var response = await blob.UploadAsync(stream, options).ConfigureAwait(false);
				_etag = response.Value.ETag;
				return true;
			}
			catch (RequestFailedException ex)
			{
				if (ex.Status == (int)HttpStatusCode.PreconditionFailed)
					return false;

				throw;
			}
		}

		private static async Task<Stream> WriteDataToStreamAsync(long data)
		{
			var buffer = BitConverter.GetBytes(data);
			var stream = new MemoryStream();
			await stream.WriteAsync(buffer, 0, buffer.Length).ConfigureAwait(false);
			stream.Seek(0, SeekOrigin.Begin);
			return stream;
		}

		private static async Task<byte[]> ReadBytesFromStreamAsync(Stream stream, int initialLength)
		{
			// If we've been passed an unhelpful initial length, just
			// use 32K.
			if (initialLength < 1)
				initialLength = short.MaxValue;

			var buffer = new byte[initialLength];
			var read = 0;

			int chunk;
			while ((chunk = await stream.ReadAsync(buffer, read, buffer.Length - read).ConfigureAwait(false)) > 0)
			{
				read += chunk;

				// If we've reached the end of our buffer, check to see if there's
				// any more information
				if (read != buffer.Length)
					continue;

				var nextByte = stream.ReadByte();

				// End of stream? If so, we're done
				if (nextByte == -1)
					return buffer;

				// Nope. Resize the buffer, put in the byte we've just
				// read, and continue
				var newBuffer = new byte[buffer.Length * 2];
				Array.Copy(buffer, newBuffer, buffer.Length);
				newBuffer[read] = (byte)nextByte;
				buffer = newBuffer;
				read++;
			}

			// Buffer is now too big. Shrink it.
			var ret = new byte[read];
			Array.Copy(buffer, ret, read);
			return ret;
		}
	}
}