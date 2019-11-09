using Microsoft.Azure.ServiceBus;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ExampleAzureServiceBus.Topic.Consumer2
{
    class Program
    {
        public static ISubscriptionClient salesSubscriptionClient;

        const string AzureServiceBusConnectionString = "<azure-service-bus-url>";

        const string TopicName = "salescancelmessages";
        const string SubscriptionName = "Europe";

        static void Main(string[] args)
        {
            ReceiveAsync().GetAwaiter().GetResult();
        }

        static async Task ReceiveAsync()
        {
            salesSubscriptionClient = new SubscriptionClient(AzureServiceBusConnectionString, TopicName, SubscriptionName);

            RegisterMessageHandler();

            Console.ReadKey();

            await salesSubscriptionClient.CloseAsync();
        }

        static void RegisterMessageHandler()
        {
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                MaxConcurrentCalls = 1,
                AutoComplete = false
            };

            salesSubscriptionClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
        }

        static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            Console.WriteLine($"Received sale performance message: MessageId:{message.MessageId} CorrelationId:{message.CorrelationId} SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");
            await salesSubscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
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
    }
}
