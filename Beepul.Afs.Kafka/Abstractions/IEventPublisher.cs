namespace Beepul.Afs.Kafka.Abstractions
{
    public interface IEventPublisher<T> where T : IKafkaEvent
    {
        Task PublishAsync(string topic, T @event);
    }
}
