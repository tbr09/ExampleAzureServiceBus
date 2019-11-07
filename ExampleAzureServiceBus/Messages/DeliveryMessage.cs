using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ExampleAzureServiceBus.Messages
{
    public class DeliveryMessage
    {
        public int ProductId { get; set; }
        public int ProductAmount { get; set; }
        public string ShopLocation { get; set; }
        public DateTime SentTime { get; set; }
        public DateTime ArriveTime { get; set; }
    }
}
