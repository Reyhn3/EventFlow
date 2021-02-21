using System.Linq;
using EventFlow.Aggregates;
using EventFlow.ReadStores;
using EventFlow.TestHelpers.Aggregates;
using EventFlow.TestHelpers.Aggregates.Entities;
using EventFlow.TestHelpers.Aggregates.Events;


namespace EventFlow.AzureStorage.IntegrationTests.ReadStores.ReadModels
{
	public class AzureStorageThingyMessageReadModel : IReadModel,
		IAmReadModelFor<ThingyAggregate, ThingyId, ThingyMessageAddedEvent>,
		IAmReadModelFor<ThingyAggregate, ThingyId, ThingyMessageHistoryAddedEvent>
	{
		public string Id { get; set; }
		public string ThingyId { get; set; }
		public string Message { get; set; }

		public void Apply(IReadModelContext context, IDomainEvent<ThingyAggregate, ThingyId, ThingyMessageAddedEvent> domainEvent)
		{
			ThingyId = domainEvent.AggregateIdentity.Value;

			var thingyMessage = domainEvent.AggregateEvent.ThingyMessage;
			Id = thingyMessage.Id.Value;
			Message = thingyMessage.Message;
		}

		public void Apply(IReadModelContext context, IDomainEvent<ThingyAggregate, ThingyId, ThingyMessageHistoryAddedEvent> domainEvent)
		{
			ThingyId = domainEvent.AggregateIdentity.Value;

			var messageId = new ThingyMessageId(context.ReadModelId);
			var thingyMessage = domainEvent.AggregateEvent.ThingyMessages.Single(m => m.Id == messageId);
			Id = messageId.Value;
			Message = thingyMessage.Message;
		}

		public ThingyMessage ToThingyMessage() =>
			new ThingyMessage(
				ThingyMessageId.With(Id),
				Message);
	}
}