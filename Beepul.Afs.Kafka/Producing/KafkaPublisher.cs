using Beepul.Afs.Kafka.Abstractions;
using Beepul.Afs.Kafka.Options;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Beepul.Afs.Kafka.Producing
{
    public class KafkaPublisher<TEvent> : IEventPublisher<TEvent>, IDisposable
        where TEvent : IKafkaEvent
    {
        private readonly IProducer<string, string> _producer;

        public KafkaPublisher(IOptions<KafkaPublisherOptions> options)
        {
            var opt = options.Value;

            var config = new ProducerConfig
            {
                BootstrapServers = opt.BootstrapServers,
                ClientId = opt.ClientId,
                Acks = Acks.All,
                EnableIdempotence = opt.EnableIdempotence,
                LingerMs = opt.LingerMs,
                BatchSize = opt.BatchSize,
                MessageSendMaxRetries = opt.MessageSendMaxRetries,
                MessageTimeoutMs = opt.MessageTimeoutMs,
                CompressionType = CompressionType.Snappy
            };

            _producer = new ProducerBuilder<string, string>(config).Build();
        }
        public void Dispose()
        {
            _producer.Flush(TimeSpan.FromSeconds(10));
            _producer.Dispose();
        }

        public async Task PublishAsync(string topic, TEvent @event)
        {
            var key = @event.GetPartitionKey();
            var json = System.Text.Json.JsonSerializer.Serialize(@event);

            var message = new Message<string, string>
            {
                Key = key,
                Value = json
            };

            var result = await _producer.ProduceAsync(topic, message);

            if (result.Status != PersistenceStatus.Persisted)
                throw new InvalidOperationException(
                    $"Kafka'ga yozilmadi: topic={topic}, key={key}, status={result.Status}");
        }
    }
}
