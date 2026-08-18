using Beepul.Afs.Kafka.Abstractions;

namespace Beepul.Afs.Kafka.Serialization;

public interface IKafkaEventSerializer
{
    byte[] Serialize<TPayload>(KafkaEvent<TPayload> @event);
    KafkaEvent<TPayload> Deserialize<TPayload>(ReadOnlySpan<byte> data);
}
