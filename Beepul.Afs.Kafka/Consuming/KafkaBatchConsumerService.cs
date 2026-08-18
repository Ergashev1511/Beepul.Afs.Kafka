using System.Text;
using Beepul.Afs.Kafka.Abstractions;
using Beepul.Afs.Kafka.Options;
using Beepul.Afs.Kafka.Serialization;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Beepul.Afs.Kafka.Consuming;

public sealed class KafkaBatchConsumerService<TPayload> : BackgroundService
{
    private readonly KafkaConsumerOptions _options;
    private readonly IBatchEventHandler<TPayload> _handler;
    private readonly IKafkaEventSerializer _serializer;
    private readonly ILogger<KafkaBatchConsumerService<TPayload>> _logger;
    private readonly IProducer<string?, byte[]>? _deadLetterProducer;

    public KafkaBatchConsumerService(
        IOptions<KafkaConsumerOptions> options,
        IBatchEventHandler<TPayload> handler,
        IKafkaEventSerializer serializer,
        ILogger<KafkaBatchConsumerService<TPayload>> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options.Value;
        _handler = handler ?? throw new ArgumentNullException(nameof(handler));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        if (!string.IsNullOrWhiteSpace(_options.DeadLetterTopic))
        {
            _deadLetterProducer = new ProducerBuilder<string?, byte[]>(new ProducerConfig
            {
                BootstrapServers = _options.BootstrapServers,
                Acks = _options.DeadLetterAcks,
                EnableIdempotence = _options.DeadLetterEnableIdempotence,
                MessageTimeoutMs = _options.DeadLetterMessageTimeoutMs
            }).Build();
        }
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => Task.Run(() => RunAsync(stoppingToken), stoppingToken);

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        using var consumer = BuildConsumer();
        consumer.Subscribe(_options.Topic);

        _logger.LogInformation(
            "Kafka consumer started. Topic={Topic}, Group={GroupId}",
            _options.Topic,
            _options.GroupId);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var rawBatch = CollectBatch(consumer, cancellationToken);
                if (rawBatch.Count == 0)
                    continue;

                await ProcessBatchAsync(consumer, rawBatch, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Normal service shutdown.
        }
        finally
        {
            consumer.Close();
        }
    }

    private IConsumer<string?, byte[]> BuildConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.GroupId,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = _options.AutoOffsetReset,
            MaxPollIntervalMs = checked((int)_options.MaxPollInterval.TotalMilliseconds),
            PartitionAssignmentStrategy = _options.PartitionAssignmentStrategy
        };

        return new ConsumerBuilder<string?, byte[]>(config).Build();
    }

    private List<ConsumeResult<string?, byte[]>> CollectBatch(
        IConsumer<string?, byte[]> consumer,
        CancellationToken cancellationToken)
    {
        var batch = new List<ConsumeResult<string?, byte[]>>(
            Math.Min(_options.MaxBatchSize, _options.InitialBatchCapacity));
        var totalBytes = 0L;
        var deadline = DateTime.UtcNow + _options.BatchTimeout;

        while (batch.Count < _options.MaxBatchSize && totalBytes < _options.MaxBatchBytes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var remaining = deadline - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero)
                break;

            var result = consumer.Consume(Min(remaining, _options.ConsumePollInterval));
            if (result?.Message?.Value is null)
                continue;

            batch.Add(result);
            totalBytes += result.Message.Value.LongLength;
        }

        return batch;
    }

    private async Task ProcessBatchAsync(
        IConsumer<string?, byte[]> consumer,
        List<ConsumeResult<string?, byte[]>> rawBatch,
        CancellationToken cancellationToken)
    {
        // Pause every assigned partition so polling for group membership cannot fetch
        // records that are not part of the batch currently being processed.
        var partitions = consumer.Assignment.ToList();
        consumer.Pause(partitions);

        try
        {
            await ProcessPausedBatchAsync(consumer, rawBatch, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            var currentlyAssigned = consumer.Assignment.ToHashSet();
            var partitionsToResume = partitions.Where(currentlyAssigned.Contains).ToList();
            if (partitionsToResume.Count > 0)
                consumer.Resume(partitionsToResume);
        }
    }

    private async Task ProcessPausedBatchAsync(
        IConsumer<string?, byte[]> consumer,
        List<ConsumeResult<string?, byte[]>> rawBatch,
        CancellationToken cancellationToken)
    {
        var events = new List<KafkaEvent<TPayload>>(rawBatch.Count);
        var malformed = new List<(ConsumeResult<string?, byte[]> Record, Exception Error)>();

        foreach (var record in rawBatch)
        {
            try
            {
                events.Add(_serializer.Deserialize<TPayload>(record.Message.Value));
            }
            catch (Exception exception)
            {
                malformed.Add((record, exception));
                _logger.LogError(
                    exception,
                    "Kafka event deserialization failed. Topic={Topic}, Partition={Partition}, Offset={Offset}",
                    record.Topic,
                    record.Partition.Value,
                    record.Offset.Value);
            }
        }

        foreach (var item in malformed)
            await SendToDeadLetterAsync(item.Record, item.Error, cancellationToken).ConfigureAwait(false);

        if (events.Count > 0)
        {
            try
            {
                await HandleWithRetryAsync(consumer, events, cancellationToken).ConfigureAwait(false);
            }
            catch (PermanentException exception)
            {
                foreach (var record in rawBatch.Except(malformed.Select(x => x.Record)))
                    await SendToDeadLetterAsync(record, exception, cancellationToken).ConfigureAwait(false);
            }
        }

        await CommitWithRetryAsync(consumer, rawBatch, cancellationToken).ConfigureAwait(false);
    }

    private async Task HandleWithRetryAsync(
        IConsumer<string?, byte[]> consumer,
        IReadOnlyList<KafkaEvent<TPayload>> events,
        CancellationToken cancellationToken)
    {
        var attempts = 0;
        var delay = _options.InitialRetryDelay;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var handlerTask = _handler.HandleAsync(events, cancellationToken);
                await AwaitWithPollingAsync(consumer, handlerTask, cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (PermanentException)
            {
                throw;
            }
            catch (Exception exception)
            {
                attempts++;
                if (_options.MaxRetryAttempts > 0 && attempts >= _options.MaxRetryAttempts)
                {
                    _logger.LogCritical(
                        exception,
                        "Batch processing stopped after {AttemptCount} attempts. Offsets were not committed.",
                        attempts);
                    throw new InvalidOperationException(
                        "Kafka batch processing retries were exhausted. Offsets were not committed.",
                        exception);
                }

                _logger.LogWarning(
                    exception,
                    "Batch processing failed. Attempt={Attempt}, RetryDelayMs={RetryDelayMs}",
                    attempts,
                    delay.TotalMilliseconds);

                await DelayWithPollingAsync(consumer, delay, cancellationToken).ConfigureAwait(false);
                delay = CalculateNextDelay(delay);
            }
        }
    }

    private async Task CommitWithRetryAsync(
        IConsumer<string?, byte[]> consumer,
        IReadOnlyCollection<ConsumeResult<string?, byte[]>> rawBatch,
        CancellationToken cancellationToken)
    {
        var offsets = rawBatch
            .GroupBy(record => record.TopicPartition)
            .Select(group => new TopicPartitionOffset(
                group.Key,
                new Offset(group.Max(record => record.Offset.Value) + 1)))
            .ToList();

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                consumer.Commit(offsets);
                return;
            }
            catch (KafkaException exception)
            {
                _logger.LogError(exception, "Kafka offset commit failed; retrying without consuming new records.");
                await DelayWithPollingAsync(consumer, _options.CommitRetryDelay, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
    }

    private async Task AwaitWithPollingAsync(
        IConsumer<string?, byte[]> consumer,
        Task operation,
        CancellationToken cancellationToken)
    {
        while (!operation.IsCompleted)
        {
            var pollingDelay = Task.Delay(_options.ProcessingPollInterval, cancellationToken);
            var completed = await Task.WhenAny(operation, pollingDelay).ConfigureAwait(false);
            if (completed == operation)
                break;

            consumer.Consume(TimeSpan.Zero);
        }

        await operation.ConfigureAwait(false);
    }

    private async Task DelayWithPollingAsync(
        IConsumer<string?, byte[]> consumer,
        TimeSpan delay,
        CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + delay;
        while (DateTime.UtcNow < deadline)
        {
            var remaining = deadline - DateTime.UtcNow;
            await Task.Delay(Min(remaining, _options.ProcessingPollInterval), cancellationToken)
                .ConfigureAwait(false);
            consumer.Consume(TimeSpan.Zero);
        }
    }

    private async Task SendToDeadLetterAsync(
        ConsumeResult<string?, byte[]> record,
        Exception exception,
        CancellationToken cancellationToken)
    {
        if (_deadLetterProducer is null || string.IsNullOrWhiteSpace(_options.DeadLetterTopic))
        {
            throw new InvalidOperationException(
                "A permanent Kafka event failure occurred, but DeadLetterTopic is not configured. " +
                "The offset will not be committed.",
                exception);
        }

        var message = new Message<string?, byte[]>
        {
            Key = record.Message.Key,
            Value = record.Message.Value,
            Headers = CopyHeaders(record.Message.Headers)
        };
        AddDeadLetterHeaders(message.Headers, record, exception);

        var result = await _deadLetterProducer
            .ProduceAsync(_options.DeadLetterTopic, message, cancellationToken)
            .ConfigureAwait(false);

        if (result.Status != PersistenceStatus.Persisted)
        {
            throw new KafkaException(new Error(
                ErrorCode.Local_MsgTimedOut,
                $"Dead-letter event at {record.TopicPartitionOffset} was not persisted."));
        }
    }

    private static Headers CopyHeaders(Headers? source)
    {
        var result = new Headers();
        if (source is null)
            return result;

        foreach (var header in source)
            result.Add(header.Key, header.GetValueBytes());
        return result;
    }

    private static void AddDeadLetterHeaders(
        Headers headers,
        ConsumeResult<string?, byte[]> record,
        Exception exception)
    {
        headers.Add("dlq-reason", Encoding.UTF8.GetBytes(exception.Message));
        headers.Add("dlq-original-topic", Encoding.UTF8.GetBytes(record.Topic));
        headers.Add("dlq-original-partition", Encoding.UTF8.GetBytes(record.Partition.Value.ToString()));
        headers.Add("dlq-original-offset", Encoding.UTF8.GetBytes(record.Offset.Value.ToString()));
    }

    private static TimeSpan Min(TimeSpan left, TimeSpan right) => left <= right ? left : right;

    private TimeSpan CalculateNextDelay(TimeSpan current) => TimeSpan.FromMilliseconds(
        Math.Min(
            current.TotalMilliseconds * _options.RetryBackoffMultiplier,
            _options.MaxRetryDelay.TotalMilliseconds));

    public override void Dispose()
    {
        _deadLetterProducer?.Flush(_options.DeadLetterFlushTimeout);
        _deadLetterProducer?.Dispose();
        base.Dispose();
    }
}
