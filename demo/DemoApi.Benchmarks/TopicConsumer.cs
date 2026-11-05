using System.Diagnostics;
using Confluent.Kafka;

namespace DemoApi.Benchmarks;

/// <summary>Stands in for the system that records messages: it dedupes by key and notes when the last new id arrived.</summary>
public sealed class TopicConsumer : IDisposable
{
    private readonly HashSet<string> _received = [];

    private readonly Lock _lock = new();

    private readonly CancellationTokenSource _stop = new();

    private readonly Thread _thread;

    private readonly string _servers;

    private readonly string _topic;

    private long _lastNewMessageAt;

    private int _duplicates;

    public TopicConsumer(string servers, string topic)
    {
        _servers = servers;

        _topic = topic;

        _thread = new Thread(ConsumeLoop) { IsBackground = true };

        _thread.Start();
    }

    public long LastNewMessageAt => Volatile.Read(ref _lastNewMessageAt);

    public int Duplicates => Volatile.Read(ref _duplicates);

    private void ConsumeLoop()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _servers,
            GroupId = $"{_topic}-reader",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };

        using var consumer = new ConsumerBuilder<string, Ignore>(config).Build();

        consumer.Subscribe(_topic);

        try
        {
            while (!_stop.IsCancellationRequested)
            {
                var record = consumer.Consume(_stop.Token);

                lock (_lock)
                {
                    if (_received.Add(record.Message.Key))
                    {
                        Volatile.Write(ref _lastNewMessageAt, Stopwatch.GetTimestamp());
                    }
                    else
                    {
                        Interlocked.Increment(ref _duplicates);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Stopped by Dispose
        }

        consumer.Close();
    }

    /// <summary>Returns how many of the ids never arrived before the timeout.</summary>
    public async Task<int> WaitForAsync(IReadOnlyCollection<string> ids, TimeSpan timeout)
    {
        var deadline = Stopwatch.GetTimestamp() + (long)(timeout.TotalSeconds * Stopwatch.Frequency);

        while (true)
        {
            var missing = CountMissing(ids);

            if (missing == 0 || Stopwatch.GetTimestamp() > deadline)
            {
                return missing;
            }

            await Task.Delay(20);
        }
    }

    public Task WaitForTrailingDuplicatesAsync() => Task.Delay(TimeSpan.FromSeconds(1));

    private int CountMissing(IReadOnlyCollection<string> ids)
    {
        lock (_lock)
        {
            return ids.Count(id => !_received.Contains(id));
        }
    }

    public void Dispose()
    {
        _stop.Cancel();

        _thread.Join(TimeSpan.FromSeconds(10));

        _stop.Dispose();
    }
}
