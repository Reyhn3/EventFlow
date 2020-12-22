using System;
using EventFlow.AzureStorage.EventStores;
using Microsoft.Azure.Cosmos.Table;


namespace EventFlow.AzureStorage.Config
{
	public class AzureStorageConfiguration
	{
		public virtual string StorageAccountConnectionString { get; set; }

		/// <summary>
		///     Gets or sets the name of the Azure Storage container used for system parameters.
		///     The default value is <c>eventflow-system-params</c>.
		/// </summary>
		/// <remarks>
		///     <para>
		///         The container name <b>must</b> conform to the following rules dictated by Azure Storage for blob containers:
		///         <list type="bullet">
		///             <item>
		///                 Container names must start or end with a letter or number, and can contain only letters, numbers, and
		///                 the dash (-) character.
		///             </item>
		///             <item>
		///                 Every dash (-) character must be immediately preceded and followed by a letter or number; consecutive
		///                 dashes are not permitted in container names.
		///             </item>
		///             <item>All letters in a container name must be lowercase.</item>
		///             <item>Container names must be from 3 through 63 characters long.</item>
		///         </list>
		///     </para>
		///     <para>
		///         See the
		///         <a
		///             href="https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata#container-names">
		///             official
		///             guidelines
		///         </a>
		///         for more information.
		///     </para>
		/// </remarks>
		public virtual string SystemContainerName { get; set; } = "eventflow-system-params";

		/// <summary>
		///     Gets or sets the range of globally unique IDs to reserve when replenishing IDs.
		/// </summary>
		/// <remarks>
		///     <para>
		///         To support globally unique identities with Azure Storage tables, a single, shared counter keeps
		///         track of the current incremented value. To allow efficient parallelism, each EventFlow instance
		///         reserves a range of numbers. This reduces the number of network requests, but also risks
		///         wasting IDs that are never used. This settings must be balanced per-system to achieve a
		///         compromise between performance and gaps in the numeric range.
		///     </para>
		///     <para>
		///         The minimum value of <c>1</c> will not reserve any numeric range, but instead force the
		///         application to do a network request whenever a new ID is needed.
		///         Setting a too high value will expend any unused IDs in a reserved range when the application
		///         is e.g. restarted.
		///     </para>
		///     <para>
		///         The globally unique ID counter is of the type <c>long</c>, which should be sufficient for
		///         most long-lived high-volume systems.
		///     </para>
		/// </remarks>
		/// ´
		/// <seealso cref="IOptimisticSyncStore.GetCurrentAsync" />
		public virtual int SequenceNumberRangeSize { get; set; } = 1000;

		public virtual int SequenceNumberOptimisticConcurrencyRetries { get; set; } = 25;

		/// <summary>
		///     Gets or sets the name of the table that stores all events.
		/// </summary>
		/// <remarks>
		///     <para>
		///         The table name <b>must</b> conform to the following rules dictated by Azure Storage for tables:
		///         <list type="bullet">
		///             <item>
		///                 Table names may contain only alphanumeric characters.
		///             </item>
		///             <item>
		///                 Table names cannot begin with a numeric character.
		///             </item>
		///             <item>Table names must be from 3 to 63 characters long.</item>
		///         </list>
		///     </para>
		///     <para>
		///         See the
		///         <a
		///             href="https://docs.microsoft.com/en-us/rest/api/storageservices/Understanding-the-Table-Service-Data-Model?redirectedfrom=MSDN#table-names">
		///             official
		///             guidelines
		///         </a>
		///         for more information.
		///     </para>
		/// </remarks>
		public virtual string EventStoreTableName { get; set; } = "EventFlowEvents";

		/// <summary>
		///     Gets or sets the name of the table that stores all read models.
		/// </summary>
		/// <remarks>
		///     <para>
		///         The table name <b>must</b> conform to the following rules dictated by Azure Storage for tables:
		///         <list type="bullet">
		///             <item>
		///                 Table names may contain only alphanumeric characters.
		///             </item>
		///             <item>
		///                 Table names cannot begin with a numeric character.
		///             </item>
		///             <item>Table names must be from 3 to 63 characters long.</item>
		///         </list>
		///     </para>
		///     <para>
		///         See the
		///         <a
		///             href="https://docs.microsoft.com/en-us/rest/api/storageservices/Understanding-the-Table-Service-Data-Model?redirectedfrom=MSDN#table-names">
		///             official
		///             guidelines
		///         </a>
		///         for more information.
		///     </para>
		/// </remarks>
		public virtual string ReadStoreTableName { get; set; } = "EventFlowReadModels";

		/// <summary>
		///     Gets or sets the name of the table that stores all snapshots.
		/// </summary>
		/// <remarks>
		///     <para>
		///         The table name <b>must</b> conform to the following rules dictated by Azure Storage for tables:
		///         <list type="bullet">
		///             <item>
		///                 Table names may contain only alphanumeric characters.
		///             </item>
		///             <item>
		///                 Table names cannot begin with a numeric character.
		///             </item>
		///             <item>Table names must be from 3 to 63 characters long.</item>
		///         </list>
		///     </para>
		///     <para>
		///         See the
		///         <a
		///             href="https://docs.microsoft.com/en-us/rest/api/storageservices/Understanding-the-Table-Service-Data-Model?redirectedfrom=MSDN#table-names">
		///             official
		///             guidelines
		///         </a>
		///         for more information.
		///     </para>
		/// </remarks>
		public virtual string SnapshotStoreTableName { get; set; } = "EventFlowSnapshots";

		public void Validate()
		{
			if (string.IsNullOrWhiteSpace(StorageAccountConnectionString))
				throw new ArgumentNullException(nameof(StorageAccountConnectionString), "The storage account connection string must be set");

			if (string.IsNullOrWhiteSpace(nameof(SystemContainerName)))
				throw new ArgumentNullException(nameof(SystemContainerName));

			if (3 > SystemContainerName.Length || SystemContainerName.Length > 63)
				throw new ArgumentOutOfRangeException(nameof(SystemContainerName), SystemContainerName,
					"The system container name must be 3-63 characters long");

			if (SequenceNumberRangeSize < 1)
				throw new ArgumentOutOfRangeException(nameof(SequenceNumberRangeSize), SequenceNumberRangeSize,
					$"{nameof(SequenceNumberRangeSize)} must be 1 or greater");

			if (SequenceNumberOptimisticConcurrencyRetries < 1)
				throw new ArgumentOutOfRangeException(nameof(SequenceNumberOptimisticConcurrencyRetries), SequenceNumberOptimisticConcurrencyRetries,
					$"{nameof(SequenceNumberOptimisticConcurrencyRetries)} must be 1 or greater");

			if (string.IsNullOrWhiteSpace(nameof(EventStoreTableName)))
				throw new ArgumentNullException(nameof(EventStoreTableName));

			NameValidator.ValidateTableName(EventStoreTableName);

			if (string.IsNullOrWhiteSpace(nameof(ReadStoreTableName)))
				throw new ArgumentNullException(nameof(ReadStoreTableName));

			NameValidator.ValidateTableName(ReadStoreTableName);

			if (string.IsNullOrWhiteSpace(nameof(SnapshotStoreTableName)))
				throw new ArgumentNullException(nameof(SnapshotStoreTableName));

			NameValidator.ValidateTableName(SnapshotStoreTableName);
		}
	}
}