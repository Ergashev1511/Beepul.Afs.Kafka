namespace Beepul.Afs.Kafka.Options
{
    public sealed class KafkaPublisherOptions
    {
        public string BootstrapServers { get; set; } = default!;
        public string ClientId { get; set; } = "beepul-afs-publisher";

        public int LingerMs { get; set; } = 10;
        public int BatchSize { get; set; } = 512 * 1024;
        public bool EnableIdempotence { get; set; } = true;
        public int MessageSendMaxRetries { get; set; } = 5;
        public int MessageTimeoutMs { get; set; } = 30000;
    }
}
