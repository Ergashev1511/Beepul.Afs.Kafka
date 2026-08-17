namespace Beepul.Afs.Kafka.Abstractions
{
    public interface IBatchEventHandler<T>
    {
        Task HandleAsync(IReadOnlyList<T> batch, CancellationToken ct);
    }
}
