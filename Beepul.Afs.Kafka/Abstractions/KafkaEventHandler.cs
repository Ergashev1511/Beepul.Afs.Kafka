namespace Beepul.Afs.Kafka.Abstractions;

public abstract class KafkaEventHandler<TPayload> : IBatchEventHandler<TPayload>
{
    public abstract Task HandleAsync(
        IReadOnlyList<KafkaEvent<TPayload>> batch,
        CancellationToken cancellationToken);

    protected static PermanentException Permanent(string message, Exception? innerException = null)
        => new(message, innerException);
}
