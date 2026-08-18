using Microsoft.Extensions.Options;

namespace Beepul.Afs.Kafka.Options;

internal sealed class KafkaPublisherOptionsValidator : IValidateOptions<KafkaPublisherOptions>
{
    public ValidateOptionsResult Validate(string? name, KafkaPublisherOptions options)
    {
        var errors = new List<string>();
        if (string.IsNullOrWhiteSpace(options.BootstrapServers)) errors.Add("BootstrapServers is required.");
        if (string.IsNullOrWhiteSpace(options.ClientId)) errors.Add("ClientId is required.");
        if (options.LingerMs < 0) errors.Add("LingerMs cannot be negative.");
        if (options.BatchSizeBytes < 1) errors.Add("BatchSizeBytes must be positive.");
        if (options.EnableIdempotence && options.Acks != Confluent.Kafka.Acks.All)
            errors.Add("Acks must be All when EnableIdempotence is enabled.");
        if (options.MessageSendMaxRetries < 0) errors.Add("MessageSendMaxRetries cannot be negative.");
        if (options.MessageTimeoutMs < 1) errors.Add("MessageTimeoutMs must be positive.");
        if (options.FlushTimeout <= TimeSpan.Zero) errors.Add("FlushTimeout must be positive.");
        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }
}
