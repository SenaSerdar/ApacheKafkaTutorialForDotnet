using Confluent.Kafka;

namespace ApacheKafkaTutorialForDotnet;

public class Program
{
    static void Producer()
    {
        var topicName = Environment.GetEnvironmentVariable("TOPIC_NAME");
        var kafkaUrl = Environment.GetEnvironmentVariable("KAFKA_URL");

        var config = new ProducerConfig() { BootstrapServers = kafkaUrl };

        using var producer = new ProducerBuilder<Null, string>(config).Build();
        while (true)
        {
            Console.Write("Enter message: ");
            var text = Console.ReadLine();

            var message = new Message<Null, string> { Value = text };
            var result = producer.ProduceAsync(topicName, message).GetAwaiter().GetResult();
            Console.WriteLine($"Sended = {result.Value} Delivered to '{result.TopicPartitionOffset}'");
        }
    }
}