using EventFlow.Exceptions;
using EventFlow.ValueObjects;
using Newtonsoft.Json;


namespace EventFlow.AzureStorage.IntegrationTests.Domain.ValueObjects
{
	[JsonConverter(typeof(SingleValueObjectConverter))]
	internal class FundQuantity : SingleValueObject<decimal>
	{
		public FundQuantity(decimal value)
			: base(value)
		{
			if (value < decimal.Zero)
				throw DomainError.With("Quantity must not be less than zero");
		}
	}
}