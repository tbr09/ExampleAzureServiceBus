using Microsoft.Azure.ServiceBus;
using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ExampleAzureServiceBus.Topic.Consumer.Filter
{
    class Program
    {
        public static ISubscriptionClient subscriptionClient;
        const string AzureServiceBusConnectionString = "Endpoint=sb://az203learning.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=CGkAvq6OG/eA462WU2cZqTcA8AJOthkaJhGBeK2rN1I=";
        const string TopicName = "salesperformancemessages";
        const string SubscriptionName = "Europe";

        static void Main(string[] args)
        {
            ReceiveAsync().GetAwaiter().GetResult();
        }

        static async Task ReceiveAsync()
        {
            subscriptionClient = new SubscriptionClient(AzureServiceBusConnectionString, TopicName, SubscriptionName);

            await AddSQLFilters();

            RegisterMessageHandler();

            Console.ReadKey();

            await subscriptionClient.CloseAsync();
        }

        static void RegisterMessageHandler()
        {
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                MaxConcurrentCalls = 1,
                AutoComplete = false
            };

            subscriptionClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);

        }

        static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            Console.WriteLine($"Received sale performance message: SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");
            await subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
        }

        static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }

        #region Filters
        private static async Task AddSQLFilters()
        {
            var rules = await subscriptionClient.GetRulesAsync();
            if (!rules.Any(r => r.Name == "PriceGreaterThan100"))
            {
                var filter = new SqlFilter("price > 100");
                await subscriptionClient.AddRuleAsync("PriceGreaterThan100", filter);
            }
        }

        private static async Task AddBooleanFilters(bool _catch)
        {
            var rules = await subscriptionClient.GetRulesAsync();

            if (_catch)
            {
                if (!rules.Any(r => r.Name == "CatchAll"))
                {
                    var catchAllFilter = new TrueFilter();
                    await subscriptionClient.AddRuleAsync("CatchAll", catchAllFilter);
                }
            }
            else
            {
                if (!rules.Any(r => r.Name == "CatchNothing"))
                {
                    var catchNothingFilter = new FalseFilter();
                    await subscriptionClient.AddRuleAsync("CatchNothing", catchNothingFilter);
                }
            }
        }

        private static async Task AddCorrelationFilter(string region)
        {

            var rules = await subscriptionClient.GetRulesAsync();

            if (!rules.Any(r => r.Name == "CorrelationRule"))
            {
                var correlationFilter = new CorrelationFilter("Region");
                await subscriptionClient.AddRuleAsync("CorrelationRule", correlationFilter);
            }
        }
        #endregion
    }
}
