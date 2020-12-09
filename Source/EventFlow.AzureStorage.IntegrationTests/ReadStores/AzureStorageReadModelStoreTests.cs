using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.Aggregates;
using EventFlow.AzureStorage.Config;
using EventFlow.AzureStorage.Extensions;
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
				.UseAzureStorageReadModel<FundReadModel>()
				.UseAzureStorage()
				.UseAzureStorageEventStore()
				.ConfigureAzureStorage(new AzureStorageConfiguration
					{
						StorageAccountConnectionString = "UseDevelopmentStorage=true",
						SystemContainerName = "eventflow-system-params",
						SequenceNumberRangeSize = 100,
						SequenceNumberOptimisticConcurrencyRetries = 25,
						EventStoreTableName = "EventFlowEvents",
						ReadStoreTableName = "EventFlowReadModels"
					})

				// Since the UpdateAsync-method of ReadStoreManager is protected,
				// use a wrapper to expose it. Replace the original registration
				// with the wrapper.
				.RegisterServices(rs => rs.Register<IReadStoreManager<FundReadModel>, ReadStoreManagerWrapper<FundAggregate, FundId, IAzureStorageReadModelStore<FundReadModel>, FundReadModel>>(keepDefault: false))
				
				.CreateResolver();

			_target = _resolver.Resolve<IReadModelStore<FundReadModel>>();
			_target.ShouldBeOfType<AzureStorageReadModelStore<FundReadModel>>();
		}

		[Test]
		public async Task UpdateAsync_should_insert_the_read_model_if_it_is_new()
		{
			var readModelId = "test-model";
			var aggregateSequenceNumber = 1;

			var updates = new[]
				{
					new ReadModelUpdate(
						readModelId, 
						new []
						{
							new DomainEvent<FundAggregate, FundId, FundSharesBought>(
								new FundSharesBought(new FundQuantity(1)), 
								new Metadata(), 
								DateTimeOffset.Now, 
								new FundId("test-aggregate"), 
								aggregateSequenceNumber) 
						}), 
				};
			var contextFactory = new ReadModelContextFactory(_resolver);
			var readStoreManager = _resolver.Resolve<IReadStoreManager<FundReadModel>>();
			var readStoreManagerWrapper = readStoreManager as ReadStoreManagerWrapper<FundAggregate, FundId, IAzureStorageReadModelStore<FundReadModel>, FundReadModel>;


			await _target.UpdateAsync(updates, contextFactory, readStoreManagerWrapper.TestUpdateAsync, CancellationToken.None);
			
			
			Assert.Fail("Test not implemented");
		}

		[Test]
		public async Task UpdateAsync_should_update_the_read_model_if_it_already_exists()
		{
			Assert.Fail("Test not implemented");
		}

		[Test]
		public async Task UpdateAsync_should_delete_the_read_model_if_it_is_marked_for_deletion()
		{
			Assert.Fail("Test not implemented");
		}

		[Test]
		public async Task GetAsync_should_retrieve_the_specified_read_model()
		{
			var result = await _target.GetAsync("test", CancellationToken.None);
			result.ReadModel.ShouldNotBeNull();
		}

		[Test]
		public async Task DeleteAsync_should_delete_the_specified_read_model()
		{
			Assert.Fail("Test not implemented");
		}

		[Test]
		public async Task DeleteAllAsync_should_delete_all_read_models_of_the_specified_type()
		{
			Assert.Fail("Test not implemented");
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
			{
				return base.UpdateAsync(readModelContext, domainEvents, readModelEnvelope, cancellationToken);
			}
		} 
	}
}