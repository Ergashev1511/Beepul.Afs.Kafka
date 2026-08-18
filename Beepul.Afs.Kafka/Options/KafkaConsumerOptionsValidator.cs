using Microsoft.Extensions.Options;

namespace Beepul.Afs.Kafka.Options;

internal sealed class KafkaConsumerOptionsValidator : IValidateOptions<KafkaConsumerOptions>
{
    public ValidateOptionsResult Validate(string? name, KafkaConsumerOptions options)
    {
        var errors = new List<string>();
        if (string.IsNullOrWhiteSpace(options.BootstrapServers)) errors.Add("BootstrapServers is required.");
        if (string.IsNullOrWhiteSpace(options.GroupId)) errors.Add("GroupId is required.");
        if (string.IsNullOrWhiteSpace(options.Topic)) errors.Add("Topic is required.");
        if (options.MaxBatchSize < 1) errors.Add("MaxBatchSize must be positive.");
        if (options.InitialBatchCapacity < 1) errors.Add("InitialBatchCapacity must be positive.");
        if (options.MaxBatchBytes < 1) errors.Add("MaxBatchBytes must be positive.");
        if (options.BatchTimeout <= TimeSpan.Zero) errors.Add("BatchTimeout must be positive.");
        if (options.ConsumePollInterval <= TimeSpan.Zero) errors.Add("ConsumePollInterval must be positive.");
        if (options.ProcessingPollInterval <= TimeSpan.Zero) errors.Add("ProcessingPollInterval must be positive.");
        if (options.MaxRetryAttempts < 0) errors.Add("MaxRetryAttempts cannot be negative.");
        if (options.InitialRetryDelay <= TimeSpan.Zero) errors.Add("InitialRetryDelay must be positive.");
        if (options.MaxRetryDelay < options.InitialRetryDelay) errors.Add("MaxRetryDelay cannot be less than InitialRetryDelay.");
        if (!double.IsFinite(options.RetryBackoffMultiplier) || options.RetryBackoffMultiplier < 1)
            errors.Add("RetryBackoffMultiplier must be a finite number greater than or equal to 1.");
        if (options.MaxPollInterval <= options.BatchTimeout) errors.Add("MaxPollInterval must be greater than BatchTimeout.");
        if (options.MaxPollInterval.TotalMilliseconds > int.MaxValue) errors.Add("MaxPollInterval is too large for Kafka.");
        if (options.CommitRetryDelay <= TimeSpan.Zero) errors.Add("CommitRetryDelay must be positive.");
        if (options.DeadLetterMessageTimeoutMs < 1) errors.Add("DeadLetterMessageTimeoutMs must be positive.");
        if (options.DeadLetterFlushTimeout <= TimeSpan.Zero) errors.Add("DeadLetterFlushTimeout must be positive.");
        if (options.DeadLetterEnableIdempotence && options.DeadLetterAcks != Confluent.Kafka.Acks.All)
            errors.Add("DeadLetterAcks must be All when DeadLetterEnableIdempotence is enabled.");
        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }
}
