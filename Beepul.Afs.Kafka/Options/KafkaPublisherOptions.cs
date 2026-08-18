using Confluent.Kafka;

namespace Beepul.Afs.Kafka.Options;

public sealed class KafkaPublisherOptions
{
    public const string DefaultSectionName = "Kafka:Publisher";

    public string BootstrapServers { get; set; } = string.Empty;
    public string ClientId { get; set; } = "beepul-afs-publisher";
    public int LingerMs { get; set; } = 10;
    public int BatchSizeBytes { get; set; } = 512 * 1024;
    public Acks Acks { get; set; } = Acks.All;
    public CompressionType CompressionType { get; set; } = CompressionType.Snappy;
    public bool EnableIdempotence { get; set; } = true;
    public int MessageSendMaxRetries { get; set; } = 5;
    public int MessageTimeoutMs { get; set; } = 30_000;
    public TimeSpan FlushTimeout { get; set; } = TimeSpan.FromSeconds(10);
}
