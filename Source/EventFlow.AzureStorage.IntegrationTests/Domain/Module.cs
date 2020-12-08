using EventFlow.Configuration;
using EventFlow.Extensions;


namespace EventFlow.AzureStorage.IntegrationTests.Domain
{
	internal class Module : IModule
	{
		public void Register(IEventFlowOptions eventFlowOptions)
		{
			eventFlowOptions
				.AddDefaults(typeof(Module).Assembly);
		}
	}
}