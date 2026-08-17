namespace Beepul.Afs.Kafka.Abstractions
{
    public abstract class KafkaEventHandler<TEvent> : IBatchEventHandler<TEvent>
        where TEvent : IKafkaEvent
    {
        protected abstract Task HandleBatchAsync(IReadOnlyList<TEvent> batch, CancellationToken ct);

        Task IBatchEventHandler<TEvent>.HandleAsync(IReadOnlyList<TEvent> batch, CancellationToken ct)
            => HandleBatchAsync(batch, ct);

        protected PartialBatchFailure Partial(IDictionary<int, Exception> failedIndices)
            => new PartialBatchFailure(new Dictionary<int, Exception>(failedIndices));

        protected PermanentException Permanent(string message, Exception? inner = null)
            => new PermanentException(message, inner);
    }
}
