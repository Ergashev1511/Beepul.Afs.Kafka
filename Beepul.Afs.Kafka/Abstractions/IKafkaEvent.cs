namespace Beepul.Afs.Kafka.Abstractions
{
    public interface IKafkaEvent
    {
        string GetPartitionKey();
    }
}
