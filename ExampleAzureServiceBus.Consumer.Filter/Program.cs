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
        public static ISubscriptionClient salesCancelSubscriptionClient;

        const string AzureServiceBusConnectionString = "<azure-service-bus-url>";

        const string TopicName = "salescancelmessages";
        const string SubscriptionName = "salescancelsubscription";

        static void Main(string[] args)
        {
            ReceiveAsync().GetAwaiter().GetResult();
        }

        static async Task ReceiveAsync()
        {
            salesCancelSubscriptionClient = new SubscriptionClient(AzureServiceBusConnectionString, TopicName, SubscriptionName);

            //await AddSQLFilters();
            await AddCorrelationFilter("chicago");

            RegisterMessageHandler();

            Console.ReadKey();

            await salesCancelSubscriptionClient.RemoveRuleAsync("CorrelationRule");
            await salesCancelSubscriptionClient.CloseAsync();
        }

        static void RegisterMessageHandler()
        {
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                MaxConcurrentCalls = 1,
                AutoComplete = false,
            };

            salesCancelSubscriptionClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
        }

        static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            Console.WriteLine($"Received sale cancel message: MessageId:{message.MessageId} CorrelationId:{message.CorrelationId} SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");
            await salesCancelSubscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
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
            var rules = await salesCancelSubscriptionClient.GetRulesAsync();
            if (!rules.Any(r => r.Name == "PriceGreaterThan100"))
            {
                var filter = new SqlFilter("Price > 100");
                await salesCancelSubscriptionClient.AddRuleAsync("PriceGreaterThan100", filter);
            }
        }

        private static async Task AddBooleanFilters(bool _catch)
        {
            var rules = await salesCancelSubscriptionClient.GetRulesAsync();

            if (_catch)
            {
                if (!rules.Any(r => r.Name == "CatchAll"))
                {
                    var catchAllFilter = new TrueFilter();
                    await salesCancelSubscriptionClient.AddRuleAsync("CatchAll", catchAllFilter);
                }
            }
            else
            {
                if (!rules.Any(r => r.Name == "CatchNothing"))
                {
                    var catchNothingFilter = new FalseFilter();
                    await salesCancelSubscriptionClient.AddRuleAsync("CatchNothing", catchNothingFilter);
                }
            }
        }

        private static async Task AddCorrelationFilter(string region)
        {
            var rules = await salesCancelSubscriptionClient.GetRulesAsync();

            if (!rules.Any(r => r.Name == "CorrelationRule"))
            {
                var correlationFilter = new CorrelationFilter(region);
                await salesCancelSubscriptionClient.AddRuleAsync("CorrelationRule", correlationFilter);
            }
        }
        #endregion
    }
}
