namespace Beepul.Afs.Kafka.Abstractions;

/// <summary>
/// Transport envelope shared by every event published through Kafka.
/// </summary>
public sealed record KafkaEvent<TPayload>
{
    public KafkaEvent(
        Guid eventId,
        string eventType,
        DateTimeOffset occurredAt,
        TPayload payload)
    {
        if (eventId == Guid.Empty)
            throw new ArgumentException("Event id cannot be empty.", nameof(eventId));
        if (string.IsNullOrWhiteSpace(eventType))
            throw new ArgumentException("Event type is required.", nameof(eventType));
        if (occurredAt == default)
            throw new ArgumentException("Occurred-at timestamp is required.", nameof(occurredAt));

        EventId = eventId;
        EventType = eventType;
        OccurredAt = occurredAt;
        Payload = payload ?? throw new ArgumentNullException(nameof(payload));
    }

    public Guid EventId { get; }
    public string EventType { get; }
    public DateTimeOffset OccurredAt { get; }
    public TPayload Payload { get; }
}
