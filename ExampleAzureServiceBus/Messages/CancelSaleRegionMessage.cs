using ExampleAzureServiceBus.Messages;
using System;

public class CancelSaleRegionMessage : CancelSaleMessage
{
    public string Region { get; set; }
}