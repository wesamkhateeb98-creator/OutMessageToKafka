using System.Text;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace DemoApi;

/// <summary>docs/blob-stream-buffer.md: reply after a shared Append every 100ms. Azurite here, so its latency is not Azure's.</summary>
public sealed class BlobBuffer : BackgroundService, IMessageAcceptor, IRelaySource
{
    private const int MaxReadBytes = 1 << 20;

    private readonly BlobContainerClient _container;

    private readonly AppendBlobClient _log;

    private readonly BlobClient _checkpoint;

    private readonly GroupCommitQueue _queue;

    private long _offset;

    private long _pendingOffset;

    public BlobBuffer(DemoOptions options)
    {
        _container = new BlobContainerClient(options.BlobConnection, "demo");

        _log = _container.GetAppendBlobClient($"{options.Topic}/{Path.GetFileName(options.DataDirectory)}.log");

        _checkpoint = _container.GetBlobClient($"{options.Topic}/{Path.GetFileName(options.DataDirectory)}.checkpoint");

        _queue = new GroupCommitQueue(CommitAsync, TimeSpan.FromMilliseconds(100));
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await _container.CreateIfNotExistsAsync(cancellationToken: cancellationToken);

        await _log.CreateIfNotExistsAsync(cancellationToken: cancellationToken);

        await base.StartAsync(cancellationToken);
    }

    public Task AcceptAsync(DemoMessage message, CancellationToken ct) => _queue.EnqueueAsync(message, ct);

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(() => _queue.RunAsync(stoppingToken), stoppingToken);

    private async Task CommitAsync(IReadOnlyList<DemoMessage> batch)
    {
        using var block = new MemoryStream();

        foreach (var message in batch)
        {
            block.Write(message.ToJson());

            block.WriteByte((byte)'\n');
        }

        block.Position = 0;

        await _log.AppendBlockAsync(block);
    }

    public async Task<IReadOnlyList<DemoMessage>> ReadBatchAsync(CancellationToken ct)
    {
        var batch = new List<DemoMessage>();

        _pendingOffset = _offset;

        var properties = await _log.GetPropertiesAsync(cancellationToken: ct);

        var available = properties.Value.ContentLength - _offset;

        if (available <= 0)
        {
            await Task.Delay(50, ct);

            return batch;
        }

        var range = new HttpRange(_offset, Math.Min(available, MaxReadBytes));

        var download = await _log.DownloadContentAsync(new BlobDownloadOptions { Range = range }, ct);

        var bytes = download.Value.Content.ToMemory();

        var position = 0;

        while (true)
        {
            var end = bytes.Span[position..].IndexOf((byte)'\n');

            if (end < 0)
            {
                break;
            }

            batch.Add(DemoMessage.FromJson(bytes.Span.Slice(position, end)));

            position += end + 1;
        }

        _pendingOffset = _offset + position;

        return batch;
    }

    public async Task AcknowledgeAsync(CancellationToken ct)
    {
        _offset = _pendingOffset;

        var content = BinaryData.FromBytes(Encoding.UTF8.GetBytes(_offset.ToString()));

        await _checkpoint.UploadAsync(content, overwrite: true, ct);
    }
}
