namespace Beepul.Afs.Kafka.Abstractions;

public sealed record KafkaPublishRequest<TPayload>
{
    public KafkaPublishRequest(string topic, KafkaEvent<TPayload> @event, string? key = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        Topic = topic;
        Event = @event ?? throw new ArgumentNullException(nameof(@event));
        Key = string.IsNullOrWhiteSpace(key) ? null : key;
    }

    public string Topic { get; }
    public KafkaEvent<TPayload> Event { get; }
    public string? Key { get; }
}
