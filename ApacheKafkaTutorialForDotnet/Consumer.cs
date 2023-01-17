using Confluent.Kafka;

namespace ApacheKafkaTutorialForDotnet;

public class Consumer
{
    class Program
    {
        static void Consumer()
        {
            var consumergroup = Environment.GetEnvironmentVariable("CONSUMER_GROUP");
            var topicName = Environment.GetEnvironmentVariable("TOPIC_NAME");
            var brokerList = Environment.GetEnvironmentVariable("KAFKA_URL");

            var config = new ConsumerConfig { GroupId = consumergroup, BootstrapServers = brokerList};

            using (var consumer = new ConsumerBuilder<string, string>(config)
                       .SetRebalanceHandler( (obj, e) => {
                           if (e.IsAssignment) Console.WriteLine($"Assigned partitions: [{string.Join(", ", e.Partitions)}]");
                           else Console.WriteLine($"Revoked partitions: [{string.Join(", ", e.Partitions)}]");
                       }).Build())
            {
                consumer.Subscribe(topicName);
                while (true)
                {
                    ConsumeResult<string, string> consumeResult = consumer.Consume();
                    Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Value}");
                    consumer.Commit();
                }
            }
        }
    }
}