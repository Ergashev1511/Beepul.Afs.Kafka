using Confluent.Kafka;

namespace Beepul.Afs.Kafka.Options;

public sealed class KafkaConsumerOptions
{
    public const string DefaultSectionName = "Kafka:Consumer";

    public string BootstrapServers { get; set; } = string.Empty;
    public string GroupId { get; set; } = string.Empty;
    public string Topic { get; set; } = string.Empty;
    public string? DeadLetterTopic { get; set; }

    public int MaxBatchSize { get; set; } = 50_000;
    public int InitialBatchCapacity { get; set; } = 65_536;
    public long MaxBatchBytes { get; set; } = 64L * 1024 * 1024;
    public TimeSpan BatchTimeout { get; set; } = TimeSpan.FromSeconds(3);
    public TimeSpan ConsumePollInterval { get; set; } = TimeSpan.FromMilliseconds(200);
    public TimeSpan ProcessingPollInterval { get; set; } = TimeSpan.FromSeconds(1);
    public AutoOffsetReset AutoOffsetReset { get; set; } = AutoOffsetReset.Earliest;
    public PartitionAssignmentStrategy PartitionAssignmentStrategy { get; set; }
        = PartitionAssignmentStrategy.CooperativeSticky;

    /// <summary>Zero means unlimited retries. Transient failures are never committed.</summary>
    public int MaxRetryAttempts { get; set; }
    public TimeSpan InitialRetryDelay { get; set; } = TimeSpan.FromMilliseconds(500);
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(30);
    public double RetryBackoffMultiplier { get; set; } = 2;
    public TimeSpan MaxPollInterval { get; set; } = TimeSpan.FromMinutes(15);
    public TimeSpan CommitRetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    public Acks DeadLetterAcks { get; set; } = Acks.All;
    public bool DeadLetterEnableIdempotence { get; set; } = true;
    public int DeadLetterMessageTimeoutMs { get; set; } = 30_000;
    public TimeSpan DeadLetterFlushTimeout { get; set; } = TimeSpan.FromSeconds(10);
}
