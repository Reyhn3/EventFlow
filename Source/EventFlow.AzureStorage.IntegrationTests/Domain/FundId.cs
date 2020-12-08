using System.Diagnostics;
using System.Text.RegularExpressions;
using EventFlow.Core;
using EventFlow.Exceptions;
using EventFlow.ValueObjects;
using Newtonsoft.Json;


namespace EventFlow.AzureStorage.IntegrationTests.Domain
{
	[DebuggerDisplay("{Value}")]
	[JsonConverter(typeof(SingleValueObjectConverter))]
	internal class FundId : SingleValueObject<string>, IIdentity
	{
		private static readonly Regex ValidString = new Regex("([a-zA-Z0-9_-]+)", RegexOptions.Compiled);

		public FundId(string value)
			: base(value)
		{
			if (string.IsNullOrWhiteSpace(value))
				throw DomainError.With("Identity may not be empty");

			if (!ValidString.IsMatch(value))
				throw DomainError.With("Not a valid ID");
		}

		public static implicit operator string(FundId id) => id.Value;
		public static implicit operator FundId(string id) => new FundId(id);
	}
}