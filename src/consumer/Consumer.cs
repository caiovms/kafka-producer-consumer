using Confluent.Kafka;
using System;
using System.Threading;

namespace consumer
{
    class Consumer
    {
        public static void Main(string[] args)
        {
            var configuration = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "127.0.0.1:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var builder = new ConsumerBuilder<Ignore, string>(configuration).Build();
            {
                builder.Subscribe("test-topic");
                
                var cancellationToken = new CancellationTokenSource();

                Console.CancelKeyPress += (_, e) => { e.Cancel = true; cancellationToken.Cancel(); };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumerResult = builder.Consume(cancellationToken.Token);
                            Console.WriteLine($"Consumed message '{consumerResult.Value}' at: '{consumerResult.TopicPartitionOffset}'.");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    builder.Close();
                }
            }
        }
    }
}
