using System.Text.Json;
using ZLogger;

namespace DemoApi;

/// <summary>docs/zlogger-file-relay.md: replies while the line is still in memory, and Filebeat ships the file. Can lose accepted messages.</summary>
public sealed class ZLoggerAcceptor : IMessageAcceptor, IDisposable
{
    private readonly ILoggerFactory _factory;

    private readonly ILogger _logger;

    private readonly string _topic;

    public ZLoggerAcceptor(DemoOptions options)
    {
        _topic = options.Topic;

        var path = Path.Combine(options.DataDirectory, "zlogger.log");

        _factory = LoggerFactory.Create(logging =>
        {
            logging.SetMinimumLevel(LogLevel.Information);

            logging.AddZLoggerFile(path, zlogger => zlogger.FullMode = BackgroundBufferFullMode.Block);
        });

        _logger = _factory.CreateLogger("messages");
    }

    public Task AcceptAsync(DemoMessage message, CancellationToken ct)
    {
        // Filebeat reads the topic from the line
        var line = JsonSerializer.Serialize(new { id = message.Id, topic = _topic, name = message.Name, email = message.Email });

        _logger.ZLogInformation($"{line}");

        return Task.CompletedTask;
    }

    public void Dispose() => _factory.Dispose();
}
