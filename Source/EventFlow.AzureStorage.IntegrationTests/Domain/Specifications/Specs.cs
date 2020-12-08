using System;
using EventFlow.Aggregates;
using EventFlow.Provided.Specifications;
using EventFlow.Specifications;


namespace EventFlow.AzureStorage.IntegrationTests.Domain.Specifications
{
	internal static class Specs
	{
		public static ISpecification<IAggregateRoot> AggregateIsNew { get; } = new AggregateIsNewSpecification();
		
		public static Func<decimal, ISpecification<decimal>> HasSufficientQuantity { get; } = requested => new HasSufficientQuantitySpecification(requested);
	}
}