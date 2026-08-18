using System.Text.Json;
using Beepul.Afs.Kafka.Abstractions;

namespace Beepul.Afs.Kafka.Serialization;

public sealed class SystemTextJsonKafkaEventSerializer : IKafkaEventSerializer
{
    private readonly JsonSerializerOptions _options;

    public SystemTextJsonKafkaEventSerializer(JsonSerializerOptions? options = null)
    {
        _options = options ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);
    }

    public byte[] Serialize<TPayload>(KafkaEvent<TPayload> @event)
        => JsonSerializer.SerializeToUtf8Bytes(@event, _options);

    public KafkaEvent<TPayload> Deserialize<TPayload>(ReadOnlySpan<byte> data)
        => JsonSerializer.Deserialize<KafkaEvent<TPayload>>(data, _options)
           ?? throw new JsonException("Kafka event payload was null.");
}
