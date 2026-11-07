using System.Text.Json;
using ZLogger;

namespace DemoApi;

/// <summary>The ZLogger section of appsettings.s6.json.</summary>
public sealed record DurableLogSettings(int MaxPending, long MaxFileBytes, int DurableTimeoutSeconds);

/// <summary>docs/zlogger-file-relay.md: replies only after fsync covered the line, and Filebeat ships the file.</summary>
public sealed class ZLoggerAcceptor : IMessageAcceptor, IDisposable
{
    private readonly SemaphoreSlim _pendingSlots;

    private readonly TimeSpan _durableTimeout;

    private readonly DurableLogStream _stream;

    private readonly ILoggerFactory _factory;

    private readonly ILogger _logger;

    private readonly string _topic;

    private long _lastSeq;

    public ZLoggerAcceptor(DemoOptions options, DurableLogSettings settings)
    {
        _topic = options.Topic;

        _pendingSlots = new SemaphoreSlim(settings.MaxPending, settings.MaxPending);

        _durableTimeout = TimeSpan.FromSeconds(settings.DurableTimeoutSeconds);

        _stream = new DurableLogStream(options.DataDirectory, settings.MaxFileBytes, DurableLogStream.SyncToDisk);

        _factory = LoggerFactory.Create(logging =>
        {
            logging.SetMinimumLevel(LogLevel.Information);

            logging.AddZLoggerStream(_stream, zlogger =>
            {
                zlogger.FullMode = BackgroundBufferFullMode.Block;

                // Equal to the pending limit, so ZLogger's PostSlow never blocks a request thread with Wait
                zlogger.BackgroundBufferCapacity = settings.MaxPending;
            });
        });

        _logger = _factory.CreateLogger("messages");
    }

    public async Task AcceptAsync(DemoMessage message, CancellationToken ct)
    {
        var hasSlot = await _pendingSlots.WaitAsync(0, ct);

        if (!hasSlot)
        {
            throw new InvalidOperationException("Too many messages are waiting for the disk.");
        }

        var seq = Interlocked.Increment(ref _lastSeq);

        try
        {
            var durable = _stream.RegisterAsync(seq);

            // Filebeat reads the topic from the line, and seq must stay the first field for DurableLogStream
            var line = JsonSerializer.Serialize(new { seq, id = message.Id, topic = _topic, name = message.Name, email = message.Email });

            _logger.ZLogInformation($"{line}");

            await durable.WaitAsync(_durableTimeout, ct);
        }
        catch
        {
            _stream.Forget(seq);

            throw;
        }
        finally
        {
            _pendingSlots.Release();
        }
    }

    public void Dispose()
    {
        _factory.Dispose();

        _pendingSlots.Dispose();
    }
}
