using Beepul.Afs.Kafka.Abstractions;
using Beepul.Afs.Kafka.Options;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text;
using System.Text.Json;

namespace Beepul.Afs.Kafka.Consuming
{
    public class KafkaBatchConsumerService<TEvent> : BackgroundService
        where TEvent : IKafkaEvent
    {
        private readonly KafkaConsumerOptions _options;
        private readonly IBatchEventHandler<TEvent> _handler;
        private readonly ILogger<KafkaBatchConsumerService<TEvent>> _logger;
        private readonly IProducer<string, string>? _dlqProducer;

        public KafkaBatchConsumerService(
            IOptions<KafkaConsumerOptions> options,
            IBatchEventHandler<TEvent> handler,
            ILogger<KafkaBatchConsumerService<TEvent>> logger)
        {
            _options = options.Value;
            _handler = handler;
            _logger = logger;

            if (!string.IsNullOrEmpty(_options.DlqTopic))
            {
                _dlqProducer = new ProducerBuilder<string, string>(new ProducerConfig
                {
                    BootstrapServers = _options.BootstrapServers,
                    Acks = Acks.All,
                    EnableIdempotence = true
                }).Build();
            }
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
            => Task.Run(() => RunLoop(stoppingToken), stoppingToken);

        private void RunLoop(CancellationToken ct)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _options.BootstrapServers,
                GroupId = _options.GroupId,
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                MaxPollIntervalMs = 300000,
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
            };

            using var consumer = new ConsumerBuilder<string, string>(config).Build();
            consumer.Subscribe(_options.Topic);

            _logger.LogInformation("Consumer boshlandi: topic={Topic} group={Group}", _options.Topic, _options.GroupId);

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    List<ConsumeResult<string, string>> rawBatch;
                    try
                    {
                        rawBatch = CollectBatch(consumer, ct);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Batch fetch xato");
                        continue;
                    }

                    if (rawBatch.Count == 0) continue;

                    var items = new List<TEvent>(rawBatch.Count);
                    var validRaw = new List<ConsumeResult<string, string>>(rawBatch.Count);

                    foreach (var r in rawBatch)
                    {
                        try
                        {
                            var value = JsonSerializer.Deserialize<TEvent>(r.Message.Value);
                            if (value != null)
                            {
                                items.Add(value);
                                validRaw.Add(r);
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Deserialize xato, offset={Offset}", r.Offset);
                            SendToDlq(r, ex);
                        }
                    }

                    ProcessWithRetry(consumer, items, validRaw, rawBatch, ct);
                }
            }
            finally
            {
                consumer.Close();
            }
        }

        private List<ConsumeResult<string, string>> CollectBatch(IConsumer<string, string> consumer, CancellationToken ct)
        {
            var batch = new List<ConsumeResult<string, string>>(_options.MaxBatchSize);
            var sw = System.Diagnostics.Stopwatch.StartNew();

            while (batch.Count < _options.MaxBatchSize && sw.ElapsedMilliseconds < _options.BatchTimeoutMs)
            {
                ct.ThrowIfCancellationRequested();

                var timeLeft = _options.BatchTimeoutMs - (int)sw.ElapsedMilliseconds;
                if (timeLeft <= 0) break;

                var result = consumer.Consume(TimeSpan.FromMilliseconds(Math.Min(timeLeft, 200)));
                if (result?.Message != null)
                    batch.Add(result);
            }

            return batch;
        }

        private void ProcessWithRetry(
            IConsumer<string, string> consumer,
            List<TEvent> items,
            List<ConsumeResult<string, string>> validRaw,
            List<ConsumeResult<string, string>> rawBatch,
            CancellationToken ct)
        {
            if (items.Count == 0)
            {
                Commit(consumer, rawBatch);
                return;
            }

            var backoff = _options.InitialBackoffMs;
            var attempt = 0;

            while (true)
            {
                try
                {
                    _handler.HandleAsync(items, ct).GetAwaiter().GetResult();
                    Commit(consumer, rawBatch);
                    return;
                }
                catch (PartialBatchFailure pf)
                {
                    foreach (var (idx, ex) in pf.FailedIndices)
                    {
                        if (idx >= 0 && idx < validRaw.Count)
                            SendToDlq(validRaw[idx], ex);
                    }
                    Commit(consumer, rawBatch);
                    return;
                }
                catch (PermanentException ex)
                {
                    foreach (var r in validRaw) SendToDlq(r, ex);
                    Commit(consumer, rawBatch);
                    return;
                }
                catch (Exception ex)
                {
                    attempt++;
                    if (_options.MaxRetries > 0 && attempt > _options.MaxRetries)
                    {
                        _logger.LogError(ex, "Max retries tugadi, {Count} ta xabar DLQ'ga yuborilmoqda", validRaw.Count);
                        foreach (var r in validRaw) SendToDlq(r, ex);
                        Commit(consumer, rawBatch);
                        return;
                    }

                    _logger.LogWarning(ex, "Batch xato (urinish {Attempt}/{Max}), {Backoff}ms dan keyin qayta uriniladi",
                        attempt, _options.MaxRetries, backoff);

                    Thread.Sleep(backoff);
                    backoff = Math.Min(backoff * 2, _options.MaxBackoffMs);
                }
            }
        }

        private void Commit(IConsumer<string, string> consumer, List<ConsumeResult<string, string>> rawBatch)
        {
            if (rawBatch.Count == 0) return;

            var offsets = rawBatch
                .GroupBy(r => r.TopicPartition)
                .Select(g => new TopicPartitionOffset(g.Key, g.Max(x => x.Offset.Value) + 1))
                .ToList();

            try
            {
                consumer.Commit(offsets);
            }
            catch (KafkaException ex)
            {
                _logger.LogError(ex, "Commit xato");
            }
        }

        private void SendToDlq(ConsumeResult<string, string> r, Exception ex)
        {
            if (_dlqProducer == null)
            {
                _logger.LogError("DLQ sozlanmagan! Xabar tashlab yuborildi. offset={Offset} error={Error}", r.Offset, ex.Message);
                return;
            }

            var headers = new Headers
            {
                { "x-dlq-reason", Encoding.UTF8.GetBytes(ex.Message) },
                { "x-dlq-original-topic", Encoding.UTF8.GetBytes(r.Topic) },
                { "x-dlq-original-partition", Encoding.UTF8.GetBytes(r.Partition.Value.ToString()) },
                { "x-dlq-original-offset", Encoding.UTF8.GetBytes(r.Offset.Value.ToString()) }
            };

            try
            {
                _dlqProducer.Produce(_options.DlqTopic, new Message<string, string>
                {
                    Key = r.Message.Key,
                    Value = r.Message.Value,
                    Headers = headers
                });
            }
            catch (Exception dlqEx)
            {
                _logger.LogError(dlqEx, "DLQ'ga yozib bo'lmadi, offset={Offset}", r.Offset);
            }
        }

        public override void Dispose()
        {
            _dlqProducer?.Flush(TimeSpan.FromSeconds(5));
            _dlqProducer?.Dispose();
            base.Dispose();
        }
    }
}
