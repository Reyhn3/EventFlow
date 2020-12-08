using System.Collections.Generic;
using EventFlow.Specifications;


namespace EventFlow.AzureStorage.IntegrationTests.Domain.Specifications
{
	internal class HasSufficientQuantitySpecification : Specification<decimal>
	{
		private readonly decimal _available;

		public HasSufficientQuantitySpecification(decimal available)
		{
			_available = available;
		}

		protected override IEnumerable<string> IsNotSatisfiedBecause(decimal obj)
		{
			if (obj > _available)
				yield return $"the requested quantity {obj} is greater than the available quantity {_available}.";
		}
	}
}