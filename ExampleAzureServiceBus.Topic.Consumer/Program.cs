using Microsoft.Azure.ServiceBus;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ExampleAzureServiceBus.Topic.Consumer
{
    class Program
    {
        public static ISubscriptionClient salesCancelSubscriptionClient;
        public static ISubscriptionClient deliverySubscriptionClient;

        const string AzureServiceBusConnectionString = "<azure-service-bus-url>";

        const string TopicName = "salescancelmessages";
        const string SubscriptionName = "Americas";

        const string DeliveryTopicName = "deliverymessages";
        const string DeliverySubscriptionName = "DeliverySubscription";

        static void Main(string[] args)
        {
            ReceiveAsync().GetAwaiter().GetResult();
        }

        static async Task ReceiveAsync()
        {
            salesCancelSubscriptionClient = new SubscriptionClient(AzureServiceBusConnectionString, TopicName, SubscriptionName);
            deliverySubscriptionClient = new SubscriptionClient(AzureServiceBusConnectionString, DeliveryTopicName, DeliverySubscriptionName);

            RegisterMessageHandlers();

            Console.ReadKey();

            await salesCancelSubscriptionClient.CloseAsync();
        }

        static void RegisterMessageHandlers()
        {
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                MaxConcurrentCalls = 1,
                AutoComplete = false
            };

            salesCancelSubscriptionClient.RegisterMessageHandler(ProcessSaleMessagesAsync, messageHandlerOptions);
            deliverySubscriptionClient.RegisterMessageHandler(ProcessDeliveryMessagesAsync, messageHandlerOptions);
        }

        static async Task ProcessDeliveryMessagesAsync(Message message, CancellationToken token)
        {
            Console.WriteLine($"Received sale performance message: MessageId:{message.MessageId} CorrelationId:{message.CorrelationId} SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");
            await deliverySubscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
        }

        static async Task ProcessSaleMessagesAsync(Message message, CancellationToken token)
        {
            Console.WriteLine($"Received sale performance message: MessageId:{message.MessageId} CorrelationId:{message.CorrelationId} SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");
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
    }
}
