using ExampleAzureServiceBus.Messages;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
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
        private readonly ITopicClient salesCancelTopicClient;
        private readonly ITopicClient deliveryTopicClient;

        public SaleController(IConfiguration configuration)
        {
            this.queueClient = new QueueClient(configuration["AzureServiceBus:ConnectionString"], "salesmessages");
            this.salesCancelTopicClient = new TopicClient(configuration["AzureServiceBus:ConnectionString"], "salescancelmessages");
            this.deliveryTopicClient = new TopicClient(configuration["AzureServiceBus:ConnectionString"], "deliverymessages");
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
            var json = JsonConvert.SerializeObject(saleMessage);
            var message = new Message(Encoding.UTF8.GetBytes(json));
            message.UserProperties["Price"] = saleMessage.Price;
            message.Label = saleMessage.GetType().ToString();
            await salesCancelTopicClient.SendAsync(message);
            await salesCancelTopicClient.CloseAsync();
            return Accepted();
        }

        [HttpPost]
        [Route("cancel/region")]
        public async Task<IActionResult> CancelRegion(CancelSaleRegionMessage cancelSaleRegionMessage)
        {
            var json = JsonConvert.SerializeObject(cancelSaleRegionMessage);
            var message = new Message(Encoding.UTF8.GetBytes(json));
            message.CorrelationId = cancelSaleRegionMessage.Region;
            await salesCancelTopicClient.SendAsync(message);
            await salesCancelTopicClient.CloseAsync();
            return Accepted();
        }

        [HttpPost]
        [Route("delivery")]
        public async Task<IActionResult> Delivery(DeliveryMessage deliveryMessage)
        {
            string json = JsonConvert.SerializeObject(deliveryMessage);
            var message = new Message(Encoding.UTF8.GetBytes(json));
            message.Label = deliveryMessage.GetType().ToString();
            message.ContentType = "application/json";
            message.CorrelationId = deliveryMessage.ShopLocation;
            await deliveryTopicClient.SendAsync(message);
            await deliveryTopicClient.CloseAsync();
            return Accepted();
        }
    }
}
