
namespace Beepul.Afs.Kafka.Abstractions;

public interface IEventPublisher
{
    Task PublishAsync<TPayload>(
        string topic,
        KafkaEvent<TPayload> @event,
        string? key = null,
        CancellationToken cancellationToken = default);

    Task PublishBatchAsync<TPayload>(
        IReadOnlyCollection<KafkaPublishRequest<TPayload>> requests,
        CancellationToken cancellationToken = default);
}
