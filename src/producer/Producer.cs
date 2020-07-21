using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace producer
{
    class Producer
    {
        public static async Task Main()
        {
            var config = new ProducerConfig { BootstrapServers = "127.0.0.1:9092" };

            using var builder = new ProducerBuilder<Null, string>(config).Build();
            {
                try
                {
                    
                    var count = 0;

                    while (true)
                    {
                        var deliveryResult = await builder.ProduceAsync("test-topic", new Message<Null, string> { Value = $"test: {count++}" });

                        Console.WriteLine($"Delivered '{deliveryResult.Value}' to '{deliveryResult.TopicPartitionOffset} | {count}'");

                        Thread.Sleep(2000);
                    }
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }
    }
}
