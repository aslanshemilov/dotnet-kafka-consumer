using System;
using System.Text.Json;
using Confluent.Kafka;

namespace dotnet_kafka_consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            try
            {
                var consumerConfig = new ConsumerConfig
                {
                    BootstrapServers = "127.0.0.1:9092",
                    GroupId = "dotnet-users-string",
                    AutoOffsetReset = AutoOffsetReset.Earliest
                };

                using (var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build())
                {
                    consumer.Subscribe("customer-topic-1");

                    while (true)
                    {
                        //var consumeResult = consumer.Consume();
                        //Console.WriteLine("Program::Main: Received Message: {0}", consumeResult.Message.Value);

                        var consumeResult = consumer.Consume();
                        var user = JsonSerializer.Deserialize<User>(consumeResult.Message.Value);
                        Console.WriteLine("Program::Main: Received Message: Key={0}, {1} {1}", consumeResult.Message.Key, user.FirstName, user.LastName);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Program::Main: Exception: {0}", ex.Message);
            }
            finally
            {
                Console.WriteLine("Program::Main: finally block");
            }





            Console.ReadKey();
        }
    }
}
