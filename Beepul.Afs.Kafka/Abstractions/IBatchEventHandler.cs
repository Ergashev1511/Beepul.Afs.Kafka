namespace Beepul.Afs.Kafka.Abstractions;

public interface IBatchEventHandler<TPayload>
{
    /// <summary>
    /// Handles a batch atomically from Kafka's point of view. Offsets are committed
    /// only after this method completes successfully.
    /// Implementations must be idempotent by <see cref="KafkaEvent{TPayload}.EventId"/>.
    /// </summary>
    Task HandleAsync(IReadOnlyList<KafkaEvent<TPayload>> batch, CancellationToken cancellationToken);
}
