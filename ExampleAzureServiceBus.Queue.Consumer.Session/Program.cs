using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Newtonsoft.Json;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ExampleAzureServiceBus.Queue.Consumer.Session
{
    class Program
    {
        const string AzureServiceBusConnectionString = "<azure-service-bus-url>";

        const string SessionQueueName = "recipemessages";

        static async Task Main(string[] args)
        {
            await Task.WhenAll(
                  SendMessagesAsync(Guid.NewGuid().ToString(), AzureServiceBusConnectionString, SessionQueueName),
                  SendMessagesAsync(Guid.NewGuid().ToString(), AzureServiceBusConnectionString, SessionQueueName),
                  SendMessagesAsync(Guid.NewGuid().ToString(), AzureServiceBusConnectionString, SessionQueueName),
                  SendMessagesAsync(Guid.NewGuid().ToString(), AzureServiceBusConnectionString, SessionQueueName));

            var client = InitializeReceiver(AzureServiceBusConnectionString, SessionQueueName);
            Task.WaitAny(
               Task.Run(() => Console.ReadKey()),
               Task.Delay(TimeSpan.FromSeconds(10)));

            await client.CloseAsync();
        }

        static QueueClient InitializeReceiver(string connectionString, string queueName)
        {
            var client = new QueueClient(connectionString, queueName, ReceiveMode.PeekLock);

            var sessionHandlerOptions = new SessionHandlerOptions(e => LogMessageHandlerException(e))
            {
                MessageWaitTimeout = TimeSpan.FromSeconds(5),
                MaxConcurrentSessions = 1,
                AutoComplete = false
            };

            client.RegisterSessionHandler(HandleSessionMessage, sessionHandlerOptions);
                
            return client;
        }

        static async Task SendMessagesAsync(string sessionId, string connectionString, string queueName)
        {
            var sender = new MessageSender(connectionString, queueName);

            dynamic data = new[]
            {
                new {step = 1, title = "Shop"},
                new {step = 2, title = "Unpack"},
                new {step = 3, title = "Prepare"},
                new {step = 4, title = "Cook"},
                new {step = 5, title = "Eat"},
            };

            for (int i = 0; i < data.Length; i++)
            {
                var message = new Message(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data[i])))
                {
                    SessionId = sessionId,
                    ContentType = "application/json",
                    Label = "RecipeStep",
                    MessageId = i.ToString(),
                    TimeToLive = TimeSpan.FromMinutes(2)
                };
                await sender.SendAsync(message);
                lock (Console.Out)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("Message sent: Session {0}, MessageId = {1}", message.SessionId, message.MessageId);
                    Console.ResetColor();
                }
            }
        }

        private static async Task HandleSessionMessage(IMessageSession session, Message message, CancellationToken cancellationToken)
        {
            if (message.Label != null &&
                message.ContentType != null &&
                message.Label.Equals("RecipeStep", StringComparison.InvariantCultureIgnoreCase) &&
                message.ContentType.Equals("application/json", StringComparison.InvariantCultureIgnoreCase))
            {
                var body = message.Body;

                dynamic recipeStep = JsonConvert.DeserializeObject(Encoding.UTF8.GetString(body));
                lock (Console.Out)
                {
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine(
                        "Message received:  SessionId = {0}, MessageId = {1}, SequenceNumber = {2}, Content: [ step = {3}, title = {4} ]",
                        message.SessionId,
                        message.MessageId,
                        message.SystemProperties.SequenceNumber,
                        recipeStep.step,
                        recipeStep.title);
                    Console.ResetColor();
                }
                await session.CompleteAsync(message.SystemProperties.LockToken);

                if (recipeStep.step == 5)
                {
                    // end of the session!
                    await session.CloseAsync();
                }
            }
            else
            {
                await session.DeadLetterAsync(message.SystemProperties.LockToken);
            }
        }

        private static Task LogMessageHandlerException(ExceptionReceivedEventArgs e)
        {
            Console.WriteLine("Exception: \"{0}\" {1}", e.Exception.Message, e.ExceptionReceivedContext.EntityPath);
            return Task.CompletedTask;
        }
    }
}