namespace Beepul.Afs.Kafka.Options
{
    public sealed class KafkaConsumerOptions
    {
        public string BootstrapServers { get; set; } = default!;
        public string GroupId { get; set; } = default!;
        public string Topic { get; set; } = default!;
        public string? DlqTopic { get; set; }

        public int MinBatchSize { get; set; } = 3000;
        public int MaxBatchSize { get; set; } = 10000;
        public int BatchTimeoutMs { get; set; } = 3000;

        public int MaxRetries { get; set; } = 5;
        public int InitialBackoffMs { get; set; } = 500;
        public int MaxBackoffMs { get; set; } = 30000;
    }
}
