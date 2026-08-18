using System.Text;
using Beepul.Afs.Kafka.Abstractions;
using Beepul.Afs.Kafka.Options;
using Beepul.Afs.Kafka.Serialization;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Beepul.Afs.Kafka.Producing;

public sealed class KafkaPublisher : IEventPublisher, IDisposable
{
    private readonly IProducer<string?, byte[]> _producer;
    private readonly IKafkaEventSerializer _serializer;
    private readonly TimeSpan _flushTimeout;

    public KafkaPublisher(
        IOptions<KafkaPublisherOptions> options,
        IKafkaEventSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(options);
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));

        var value = options.Value;
        _flushTimeout = value.FlushTimeout;
        var config = new ProducerConfig
        {
            BootstrapServers = value.BootstrapServers,
            ClientId = value.ClientId,
            Acks = value.Acks,
            EnableIdempotence = value.EnableIdempotence,
            LingerMs = value.LingerMs,
            BatchSize = value.BatchSizeBytes,
            MessageSendMaxRetries = value.MessageSendMaxRetries,
            MessageTimeoutMs = value.MessageTimeoutMs,
            CompressionType = value.CompressionType
        };

        _producer = new ProducerBuilder<string?, byte[]>(config).Build();
    }

    public async Task PublishAsync<TPayload>(
        string topic,
        KafkaEvent<TPayload> @event,
        string? key = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(@event);

        var result = await ProduceAsync(topic, @event, key, cancellationToken).ConfigureAwait(false);
        EnsurePersisted(result, @event.EventId);
    }

    public async Task PublishBatchAsync<TPayload>(
        IReadOnlyCollection<KafkaPublishRequest<TPayload>> requests,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(requests);
        if (requests.Count == 0)
            return;

        var pending = requests
            .Select(request => (
                request.Event.EventId,
                Delivery: ProduceAsync(request.Topic, request.Event, request.Key, cancellationToken)))
            .ToArray();

        await Task.WhenAll(pending.Select(item => item.Delivery)).ConfigureAwait(false);
        foreach (var item in pending)
            EnsurePersisted(item.Delivery.Result, item.EventId);
    }

    private Task<DeliveryResult<string?, byte[]>> ProduceAsync<TPayload>(
        string topic,
        KafkaEvent<TPayload> @event,
        string? key,
        CancellationToken cancellationToken)
    {
        var message = new Message<string?, byte[]>
        {
            Key = string.IsNullOrWhiteSpace(key) ? null : key,
            Value = _serializer.Serialize(@event),
            Headers = CreateHeaders(@event)
        };

        return _producer.ProduceAsync(topic, message, cancellationToken);
    }

    private static Headers CreateHeaders<TPayload>(KafkaEvent<TPayload> @event) =>
    [
        new Header("event-id", Encoding.UTF8.GetBytes(@event.EventId.ToString("D"))),
        new Header("event-type", Encoding.UTF8.GetBytes(@event.EventType))
    ];

    private static void EnsurePersisted(
        DeliveryResult<string?, byte[]> result,
        Guid eventId)
    {
        if (result.Status == PersistenceStatus.Persisted)
            return;

        throw new KafkaException(new Error(
            ErrorCode.Local_MsgTimedOut,
            $"Event '{eventId}' was not persisted. Status: {result.Status}."));
    }

    public void Dispose()
    {
        _producer.Flush(_flushTimeout);
        _producer.Dispose();
    }
}
