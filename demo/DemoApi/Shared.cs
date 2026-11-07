using System.Text.Json;
using System.Threading.Channels;
using Confluent.Kafka;

namespace DemoApi;

public sealed record IncomingMessage(string Name, string Email);

public sealed record DemoMessage(string Id, string Name, string Email)
{
    public byte[] ToJson() => JsonSerializer.SerializeToUtf8Bytes(this);

    public static DemoMessage FromJson(ReadOnlySpan<byte> json) => JsonSerializer.Deserialize<DemoMessage>(json)!;
}

/// <summary>Completes only when the message has the guarantee its solution promises.</summary>
public interface IMessageAcceptor
{
    Task AcceptAsync(DemoMessage message, CancellationToken ct);
}

/// <summary>Settings every solution uses. KafkaServers comes from appsettings.json; Topic and DataDirectory change per run.</summary>
public sealed record DemoOptions(string Topic, string DataDirectory, string KafkaServers)
{
    public static DemoOptions From(IConfiguration configuration) => new(
        Required(configuration, "Demo:Topic"),
        Required(configuration, "Demo:DataDirectory"),
        Required(configuration, "Demo:KafkaServers"));

    private static string Required(IConfiguration configuration, string key) =>
        configuration[key] ?? throw new InvalidOperationException($"{key} is required.");
}

public static class SettingsExtensions
{
    /// <summary>Binds one section of appsettings.{solution}.json, and fails at startup when it is missing.</summary>
    public static T GetRequiredSettings<T>(this IConfiguration configuration, string section) =>
        configuration.GetRequiredSection(section).Get<T>()
            ?? throw new InvalidOperationException($"Section {section} could not be read.");
}

/// <summary>The one producer shared by every request and relay: acks=all with idempotence.</summary>
public sealed class KafkaSender : IDisposable
{
    private readonly IProducer<string, byte[]> _producer;

    private readonly string _topic;

    public KafkaSender(DemoOptions options)
    {
        _topic = options.Topic;

        var config = new ProducerConfig
        {
            BootstrapServers = options.KafkaServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            LingerMs = 5,
            MessageTimeoutMs = 5000,
        };

        _producer = new ProducerBuilder<string, byte[]>(config).Build();
    }

    public Task SendAsync(DemoMessage message, CancellationToken ct)
    {
        return _producer.ProduceAsync(_topic, new Message<string, byte[]> { Key = message.Id, Value = message.ToJson() }, ct);
    }

    public Task SendBatchAsync(IReadOnlyList<DemoMessage> batch, CancellationToken ct)
    {
        return Task.WhenAll(batch.Select(message => SendAsync(message, ct)));
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));

        _producer.Dispose();
    }
}

/// <summary>Where a relay reads from. The read position moves only after Kafka acknowledged the batch.</summary>
public interface IRelaySource
{
    Task<IReadOnlyList<DemoMessage>> ReadBatchAsync(CancellationToken ct);

    Task AcknowledgeAsync(CancellationToken ct);
}

/// <summary>Reads a batch, sends it, then acknowledges it. A crash in between resends, so the consumer dedupes.</summary>
public sealed class RelayWorker(IRelaySource source, KafkaSender sender) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var batch = await source.ReadBatchAsync(stoppingToken);

                if (batch.Count == 0)
                {
                    await Task.Delay(5, stoppingToken);

                    continue;
                }

                await sender.SendBatchAsync(batch, stoppingToken);

                await source.AcknowledgeAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception)
            {
                // Kafka or the store failed: nothing was acknowledged, so the same batch is read again
                await Task.Delay(100, stoppingToken);
            }
        }
    }
}

/// <summary>Group commit: every waiting message shares one commit, and all complete only after it.</summary>
public sealed class GroupCommitQueue(Func<IReadOnlyList<DemoMessage>, Task> commitAsync, TimeSpan batchWindow, int maxBatch = 10_000)
{
    private readonly Channel<(DemoMessage Message, TaskCompletionSource Done)> _channel =
        Channel.CreateBounded<(DemoMessage, TaskCompletionSource)>(new BoundedChannelOptions(50_000)
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait,
        });

    public async Task EnqueueAsync(DemoMessage message, CancellationToken ct)
    {
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await _channel.Writer.WriteAsync((message, done), ct);

        await done.Task;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        var messages = new List<DemoMessage>(maxBatch);

        var waiting = new List<TaskCompletionSource>(maxBatch);

        while (await _channel.Reader.WaitToReadAsync(ct))
        {
            if (batchWindow > TimeSpan.Zero)
            {
                await Task.Delay(batchWindow, ct);
            }

            messages.Clear();

            waiting.Clear();

            while (messages.Count < maxBatch && _channel.Reader.TryRead(out var item))
            {
                messages.Add(item.Message);

                waiting.Add(item.Done);
            }

            try
            {
                await commitAsync(messages);

                waiting.ForEach(done => done.SetResult());
            }
            catch (Exception error)
            {
                // Nothing in this batch is durable: its requests get 503
                waiting.ForEach(done => done.SetException(error));
            }
        }
    }
}
