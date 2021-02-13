using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.Aggregates;
using EventFlow.AzureStorage.Config;
using EventFlow.AzureStorage.Connection;
using EventFlow.AzureStorage.IntegrationTests.Domain;
using EventFlow.AzureStorage.IntegrationTests.Domain.Events;
using EventFlow.AzureStorage.IntegrationTests.Domain.ValueObjects;
using EventFlow.AzureStorage.ReadStores;
using EventFlow.Configuration;
using EventFlow.Core;
using EventFlow.EventStores;
using EventFlow.Logs;
using EventFlow.ReadStores;
using EventFlow.TestHelpers;
using FakeItEasy;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.Cosmos.Table.Protocol;
using NUnit.Framework;
using Shouldly;


namespace EventFlow.AzureStorage.IntegrationTests.ReadStores
{
	[Explicit("Intended for manual verification")]
	[Category(Categories.Integration)]
	public class AzureStorageReadModelStoreTests
	{
		private IReadModelStore<FundReadModel> _target;
		private IRootResolver _resolver;

		[SetUp]
		public void PreRun()
		{
			_resolver = EventFlowOptions.New
				.RegisterModule<Module>()
				.UseAzureStorage(c =>
					{
						c.StorageAccountConnectionString = "UseDevelopmentStorage=true";
						c.ReadStoreTableName = "EventFlowReadModelsTEST";
					})
				.UseAzureStorageReadModelFor<FundReadModel>()

				// Since the UpdateAsync-method of ReadStoreManager is protected,
				// use a wrapper to expose it. Replace the original registration
				// with the wrapper.
				.RegisterServices(rs
					=> rs.Register<IReadStoreManager<FundReadModel>, ReadStoreManagerWrapper<FundAggregate, FundId, IAzureStorageReadModelStore<FundReadModel>, FundReadModel>>(
						keepDefault: false))
				.CreateResolver();

			_target = _resolver.Resolve<IReadModelStore<FundReadModel>>();
			_target.ShouldBeOfType<AzureStorageReadModelStore<FundReadModel>>();
		}

		[Test]
		public async Task UpdateAsync_should_insert_the_read_model_if_it_is_new()
		{
			// Arrange

			const string readModelId = "test-model-update-new";
			const int aggregateSequenceNumber = 1;

			var contextFactory = new ReadModelContextFactory(_resolver);
			var readStoreManager = _resolver.Resolve<IReadStoreManager<FundReadModel>>();
			var readStoreManagerWrapper = readStoreManager as ReadStoreManagerWrapper<FundAggregate, FundId, IAzureStorageReadModelStore<FundReadModel>, FundReadModel>;

			var updates = new[]
				{
					new ReadModelUpdate(
						readModelId,
						new[]
							{
								new DomainEvent<FundAggregate, FundId, FundSharesBought>(
									new FundSharesBought(new FundQuantity(1)),
									A.Dummy<Metadata>(),
									DateTimeOffset.UtcNow,
									A.Dummy<FundId>(),
									aggregateSequenceNumber)
							})
				};


			// Act

			await _target.UpdateAsync(updates, contextFactory, readStoreManagerWrapper.TestUpdateAsync, A.Dummy<CancellationToken>());


			// Assert

			var result = await _target.GetAsync(readModelId, CancellationToken.None);
			result.ShouldNotBeNull();
			result.Version.HasValue.ShouldBeTrue();
			result.Version.Value.ShouldBe(1);
		}

		[Test]
		public async Task UpdateAsync_should_update_the_read_model_if_it_already_exists()
		{
			// Arrange

			const string readModelId = "test-model-update-existing";

			var contextFactory = new ReadModelContextFactory(_resolver);
			var readStoreManager = _resolver.Resolve<IReadStoreManager<FundReadModel>>();
			var readStoreManagerWrapper = readStoreManager as ReadStoreManagerWrapper<FundAggregate, FundId, IAzureStorageReadModelStore<FundReadModel>, FundReadModel>;

			var updatesNew = new[]
				{
					new ReadModelUpdate(
						readModelId,
						new[]
							{
								new DomainEvent<FundAggregate, FundId, FundSharesBought>(
									new FundSharesBought(new FundQuantity(1)),
									A.Dummy<Metadata>(),
									DateTimeOffset.UtcNow,
									A.Dummy<FundId>(),
									1)
							})
				};

			var updatesExisting = new[]
				{
					new ReadModelUpdate(
						readModelId,
						new[]
							{
								new DomainEvent<FundAggregate, FundId, FundSharesBought>(
									new FundSharesBought(new FundQuantity(2)),
									A.Dummy<Metadata>(),
									DateTimeOffset.UtcNow,
									A.Dummy<FundId>(),
									2)
							})
				};

			await _target.UpdateAsync(updatesNew, contextFactory, readStoreManagerWrapper.TestUpdateAsync, A.Dummy<CancellationToken>());


			// Act

			await _target.UpdateAsync(updatesExisting, contextFactory, readStoreManagerWrapper.TestUpdateAsync, A.Dummy<CancellationToken>());


			// Assert

			var result = await _target.GetAsync(readModelId, CancellationToken.None);
			result.ShouldNotBeNull();
			result.Version.HasValue.ShouldBeTrue();
			result.Version.Value.ShouldBe(2);
			result.ReadModel.Quantity.ShouldBe(3);
		}

		[Test]
		public async Task UpdateAsync_should_delete_the_read_model_if_it_is_marked_for_deletion()
		{
			// Arrange

			const string readModelId = "test-model-update-delete";

			var contextFactory = new ReadModelContextFactory(_resolver);
			var readStoreManager = _resolver.Resolve<IReadStoreManager<FundReadModel>>();
			var readStoreManagerWrapper = readStoreManager as ReadStoreManagerWrapper<FundAggregate, FundId, IAzureStorageReadModelStore<FundReadModel>, FundReadModel>;

			var updatesNew = new[]
				{
					new ReadModelUpdate(
						readModelId,
						new[]
							{
								new DomainEvent<FundAggregate, FundId, FundSharesBought>(
									new FundSharesBought(new FundQuantity(1)),
									A.Dummy<Metadata>(),
									DateTimeOffset.UtcNow,
									A.Dummy<FundId>(),
									1)
							})
				};

			var updatesDelete = new[]
				{
					new ReadModelUpdate(
						readModelId,
						new[]
							{
								new DomainEvent<FundAggregate, FundId, FundSharesDeleted>(
									new FundSharesDeleted(),
									A.Dummy<Metadata>(),
									DateTimeOffset.UtcNow,
									A.Dummy<FundId>(),
									2)
							})
				};

			await _target.UpdateAsync(updatesNew, contextFactory, readStoreManagerWrapper.TestUpdateAsync, A.Dummy<CancellationToken>());


			// Act

			await _target.UpdateAsync(updatesDelete, contextFactory, readStoreManagerWrapper.TestUpdateAsync, A.Dummy<CancellationToken>());


			// Assert

			var result = await _target.GetAsync(readModelId, A.Dummy<CancellationToken>());
			result.ShouldNotBeNull();
			result.ReadModelId.ShouldBe(readModelId);
			result.ReadModel.ShouldBeNull();
			result.Version.ShouldBeNull();
		}

		[Test]
		public async Task GetAsync_should_retrieve_the_specified_read_model()
		{
			// Arrange

			const string readModelId = "test-model-get-existing";
			const int aggregateSequenceNumber = 1;

			var contextFactory = new ReadModelContextFactory(_resolver);
			var readStoreManager = _resolver.Resolve<IReadStoreManager<FundReadModel>>();
			var readStoreManagerWrapper = readStoreManager as ReadStoreManagerWrapper<FundAggregate, FundId, IAzureStorageReadModelStore<FundReadModel>, FundReadModel>;

			var updates = new[]
				{
					new ReadModelUpdate(
						readModelId,
						new[]
							{
								new DomainEvent<FundAggregate, FundId, FundSharesBought>(
									new FundSharesBought(new FundQuantity(1)),
									A.Dummy<Metadata>(),
									DateTimeOffset.UtcNow,
									A.Dummy<FundId>(),
									aggregateSequenceNumber)
							})
				};

			await _target.UpdateAsync(updates, contextFactory, readStoreManagerWrapper.TestUpdateAsync, A.Dummy<CancellationToken>());


			// Act

			var result = await _target.GetAsync("test-model-get-existing", A.Dummy<CancellationToken>());


			// Assert

			result.ReadModel.ShouldNotBeNull();
			result.Version.HasValue.ShouldBeTrue();
			result.Version.Value.ShouldBe(1);
		}

		[Test]
		public async Task DeleteAsync_should_delete_the_specified_read_model()
		{
			// Arrange

			const string readModelId = "test-model-delete";

			var contextFactory = new ReadModelContextFactory(_resolver);
			var readStoreManager = _resolver.Resolve<IReadStoreManager<FundReadModel>>();
			var readStoreManagerWrapper = readStoreManager as ReadStoreManagerWrapper<FundAggregate, FundId, IAzureStorageReadModelStore<FundReadModel>, FundReadModel>;

			var updates = new[]
				{
					new ReadModelUpdate(
						readModelId,
						new[]
							{
								new DomainEvent<FundAggregate, FundId, FundSharesBought>(
									new FundSharesBought(new FundQuantity(1)),
									A.Dummy<Metadata>(),
									DateTimeOffset.UtcNow,
									A.Dummy<FundId>(),
									1)
							})
				};

			await _target.UpdateAsync(updates, contextFactory, readStoreManagerWrapper.TestUpdateAsync, A.Dummy<CancellationToken>());


			// Act

			await _target.DeleteAsync(readModelId, A.Dummy<CancellationToken>());


			// Assert

			var result = await _target.GetAsync(readModelId, A.Dummy<CancellationToken>());
			result.ShouldNotBeNull();
			result.ReadModelId.ShouldBe(readModelId);
			result.ReadModel.ShouldBeNull();
			result.Version.ShouldBeNull();
		}

		[Test]
		public async Task DeleteAllAsync_should_delete_all_read_models_of_the_specified_type()
		{
			// Arrange

			const string readModelId = "test-model-delete-all";

			var contextFactory = new ReadModelContextFactory(_resolver);
			var readStoreManager = _resolver.Resolve<IReadStoreManager<FundReadModel>>();
			var readStoreManagerWrapper = readStoreManager as ReadStoreManagerWrapper<FundAggregate, FundId, IAzureStorageReadModelStore<FundReadModel>, FundReadModel>;

			var updates = new[]
				{
					new ReadModelUpdate(
						readModelId,
						new[]
							{
								new DomainEvent<FundAggregate, FundId, FundSharesBought>(
									new FundSharesBought(new FundQuantity(1)),
									A.Dummy<Metadata>(),
									DateTimeOffset.UtcNow,
									A.Dummy<FundId>(),
									1)
							})
				};

			await _target.UpdateAsync(updates, contextFactory, readStoreManagerWrapper.TestUpdateAsync, A.Dummy<CancellationToken>());


			// Act

			await _target.DeleteAllAsync(A.Dummy<CancellationToken>());


			// Assert

			var factory = _resolver.Resolve<IAzureStorageFactory>();
			var filter = TableQuery.GenerateFilterCondition(TableConstants.PartitionKey, QueryComparisons.Equal, nameof(FundReadModel));
			var query = new TableQuery().Where(filter).Select(new[]
				{
					TableConstants.PartitionKey,
					TableConstants.RowKey
				});
			var table = factory.CreateTableReferenceForReadStore();
			var result = table.ExecuteQuery(query);
			result.Any().ShouldBeFalse();
		}


		// This wrapper exposes the protected UpdateAsync-method for the sake of testing.
		private class ReadStoreManagerWrapper<TAggregate, TIdentity, TReadModelStore, TReadModel>
			: SingleAggregateReadStoreManager<TAggregate, TIdentity, TReadModelStore, TReadModel>
			where TAggregate : IAggregateRoot<TIdentity>
			where TIdentity : IIdentity
			where TReadModelStore : IReadModelStore<TReadModel>
			where TReadModel : class, IReadModel
		{
			public ReadStoreManagerWrapper(
				ILog log,
				IResolver resolver,
				TReadModelStore readModelStore,
				IReadModelDomainEventApplier readModelDomainEventApplier,
				IReadModelFactory<TReadModel> readModelFactory,
				IEventStore eventStore)
				: base(log, resolver, readModelStore, readModelDomainEventApplier, readModelFactory, eventStore)
			{}

			public Task<ReadModelUpdateResult<TReadModel>> TestUpdateAsync(
				IReadModelContext readModelContext,
				IReadOnlyCollection<IDomainEvent> domainEvents,
				ReadModelEnvelope<TReadModel> readModelEnvelope,
				CancellationToken cancellationToken)
				=> base.UpdateAsync(readModelContext, domainEvents, readModelEnvelope, cancellationToken);
		}
	}
}