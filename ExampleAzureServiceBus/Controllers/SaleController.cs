using ExampleAzureServiceBus.Messages;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExampleAzureServiceBus.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class SaleController : ControllerBase
    {
        private readonly IQueueClient queueClient;
        private readonly ITopicClient topicClient;

        public SaleController(IConfiguration configuration)
        {
            this.queueClient = queueClient = new QueueClient(configuration["AzureServiceBus:ConnectionString"], "salesmessages");
            this.topicClient = new TopicClient(configuration["AzureServiceBus:ConnectionString"], "salesperformancemessages");
        }

        [HttpPost]
        public async Task<IActionResult> Post(SaleMessage saleMessage)
        {
            var encodedMessage = new Message(Encoding.UTF8.GetBytes(saleMessage.Message));
            await queueClient.SendAsync(encodedMessage);
            await queueClient.CloseAsync();
            return Accepted();
        }

        [HttpPost]
        [Route("cancel")]
        public async Task<IActionResult> Cancel(CancelSaleMessage saleMessage)
        {
            var message = new Message(Encoding.UTF8.GetBytes(saleMessage.Message));
            await topicClient.SendAsync(message);
            await topicClient.CloseAsync();
            return Accepted();
        }
    }
}
