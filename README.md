# Beepul.Afs.Kafka

Reusable .NET 8 Kafka publisher and batch consumer with at-least-once delivery semantics.

To‘liq o‘zbekcha qo‘llanma: [docs/UZBEK_QOLLANMA.md](docs/UZBEK_QOLLANMA.md)

## Delivery contract

- Every event has a stable `EventId`, `EventType`, `OccurredAt`, and typed `Payload`.
- The Kafka message key is optional and is supplied separately from the event body.
- Consumer offsets are committed only after the batch handler succeeds.
- A transient handler failure is retried without committing offsets.
- Malformed events and explicit `PermanentException` failures are committed only after Kafka confirms their delivery to the dead-letter topic.
- A handler can run more than once when ClickHouse succeeds but the Kafka offset commit fails. Handlers must therefore be idempotent by `EventId`.

Kafka and ClickHouse do not share a transaction. The package guarantees at-least-once processing, not exactly-once storage.

## Publish

```csharp
builder.Services.AddKafkaPublisher(builder.Configuration);

var envelope = new KafkaEvent<SessionStarted>(
    eventId: eventId,                 // generated once, before any publish retry
    eventType: "session.started",     // stable contract name
    occurredAt: DateTimeOffset.UtcNow,
    payload: new SessionStarted(sessionId, userId));

await publisher.PublishAsync(
    "afs.session.events",
    envelope,
    key: $"session:{sessionId}",      // null when ordering is not required
    cancellationToken);
```

For high-throughput producers, create `KafkaPublishRequest<TPayload>` items and call `PublishBatchAsync`. The package queues the deliveries together so the singleton native producer can pipeline and batch them efficiently.

Use the same key for events whose order must be preserved. For example, all `create`, `auth`, and `pay` events for one transfer should use `transfer:{TransferId}`.

## Consume

```csharp
public sealed class SessionBatchHandler : KafkaEventHandler<SessionStarted>
{
    public override async Task HandleAsync(
        IReadOnlyList<KafkaEvent<SessionStarted>> batch,
        CancellationToken cancellationToken)
    {
        // Insert the batch into ClickHouse idempotently by EventId.
        // Throw on a transient failure: Kafka offsets remain uncommitted.
        await InsertIntoClickHouse(batch, cancellationToken);
    }
}

builder.Services.AddKafkaBatchConsumer<SessionStarted, SessionBatchHandler>(
    builder.Configuration);
```

Throw `PermanentException` only when retrying cannot fix the event data. A dead-letter topic must be configured for permanent failures; otherwise the consumer intentionally stops without committing the affected offsets.

## Configuration

```json
{
  "Kafka": {
    "Publisher": {
      "BootstrapServers": "kafka:9092",
      "ClientId": "afs-publisher",
      "LingerMs": 10,
      "BatchSizeBytes": 524288,
      "Acks": "All",
      "CompressionType": "Snappy",
      "EnableIdempotence": true,
      "MessageSendMaxRetries": 5,
      "MessageTimeoutMs": 30000,
      "FlushTimeout": "00:00:10"
    },
    "Consumer": {
      "BootstrapServers": "kafka:9092",
      "GroupId": "afs-clickhouse-writer",
      "Topic": "afs.analytics.events",
      "DeadLetterTopic": "afs.analytics.events.dlq",
      "MaxBatchSize": 50000,
      "InitialBatchCapacity": 65536,
      "MaxBatchBytes": 67108864,
      "BatchTimeout": "00:00:03",
      "ConsumePollInterval": "00:00:00.200",
      "ProcessingPollInterval": "00:00:01",
      "AutoOffsetReset": "Earliest",
      "PartitionAssignmentStrategy": "CooperativeSticky",
      "MaxRetryAttempts": 0,
      "InitialRetryDelay": "00:00:00.500",
      "MaxRetryDelay": "00:00:30",
      "RetryBackoffMultiplier": 2,
      "MaxPollInterval": "00:15:00",
      "CommitRetryDelay": "00:00:01",
      "DeadLetterAcks": "All",
      "DeadLetterEnableIdempotence": true,
      "DeadLetterMessageTimeoutMs": 30000,
      "DeadLetterFlushTimeout": "00:00:10"
    }
  }
}
```

`MaxRetryAttempts: 0` means unlimited retries. This is the recommended value for temporary ClickHouse outages because transfer events must remain in Kafka until storage recovers.

The batch closes when `MaxBatchSize`, `MaxBatchBytes`, or `BatchTimeout` is reached first. Tune these values using actual payload size, ClickHouse insert latency, Kafka consumer lag, and process memory.

## Operational requirements

- Generate `EventId` in the business transaction or outbox and reuse it for every retry.
- Use an outbox when a database change and event publication must be coordinated.
- Make ClickHouse writes idempotent by `EventId`.
- Monitor consumer lag, handler duration, retries, dead-letter volume, and process memory.
- Scale consumers through Kafka partitions and consumer-group replicas; ordering is guaranteed only within one partition.
