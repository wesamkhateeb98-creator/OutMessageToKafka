using System.Buffers.Binary;
using System.IO.Hashing;
using Microsoft.Win32.SafeHandles;

namespace DemoApi;

/// <summary>Record layout: [Length 4B][CRC 4B][Payload].</summary>
public static class WalRecord
{
    public const int HeaderSize = 8;

    public static byte[] Encode(DemoMessage message)
    {
        var payload = message.ToJson();

        var record = new byte[HeaderSize + payload.Length];

        BinaryPrimitives.WriteInt32LittleEndian(record, payload.Length);

        BinaryPrimitives.WriteUInt32LittleEndian(record.AsSpan(4), Crc32.HashToUInt32(payload));

        payload.CopyTo(record, HeaderSize);

        return record;
    }
}

/// <summary>Relay side of a WAL: reads whole, valid records up to the limit the writer allows, and saves a checkpoint.</summary>
public sealed class WalReader : IDisposable
{
    private const int MaxReadBytes = 1 << 20;

    private readonly SafeFileHandle _handle;

    private readonly FileStream _checkpoint;

    private readonly byte[] _buffer = new byte[MaxReadBytes];

    private long _offset;

    private long _pendingOffset;

    public WalReader(string logPath, string checkpointPath)
    {
        _handle = File.OpenHandle(logPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);

        _checkpoint = new FileStream(checkpointPath, FileMode.Create, FileAccess.Write, FileShare.Read, 0);
    }

    public IReadOnlyList<DemoMessage> ReadBatch(long readableLength)
    {
        var batch = new List<DemoMessage>();

        var available = (int)Math.Min(readableLength - _offset, MaxReadBytes);

        _pendingOffset = _offset;

        if (available <= 0)
        {
            return batch;
        }

        var read = RandomAccess.Read(_handle, _buffer.AsSpan(0, available), _offset);

        var position = 0;

        while (position + WalRecord.HeaderSize <= read)
        {
            var length = BinaryPrimitives.ReadInt32LittleEndian(_buffer.AsSpan(position));

            var next = position + WalRecord.HeaderSize + length;

            if (next > read)
            {
                break;
            }

            var payload = _buffer.AsSpan(position + WalRecord.HeaderSize, length);

            var crc = BinaryPrimitives.ReadUInt32LittleEndian(_buffer.AsSpan(position + 4));

            if (crc != Crc32.HashToUInt32(payload))
            {
                throw new InvalidDataException($"Bad CRC at offset {_offset + position}.");
            }

            batch.Add(DemoMessage.FromJson(payload));

            position = next;
        }

        _pendingOffset = _offset + position;

        return batch;
    }

    public void Acknowledge()
    {
        _offset = _pendingOffset;

        _checkpoint.Position = 0;

        _checkpoint.Write(BitConverter.GetBytes(_offset));

        _checkpoint.Flush(true);
    }

    public void Dispose()
    {
        _handle.Dispose();

        _checkpoint.Dispose();
    }
}

/// <summary>docs/local-wal-group-commit.md and docs/gateway-replicated-wal.md: reply after a shared fsync.</summary>
public sealed class GroupCommitWal : BackgroundService, IMessageAcceptor, IRelaySource
{
    private readonly FileStream _log;

    private readonly WalReader _reader;

    private readonly GroupCommitQueue _queue;

    private long _durableLength;

    public GroupCommitWal(DemoOptions options)
    {
        var path = Path.Combine(options.DataDirectory, "wal.log");

        _log = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read | FileShare.Delete, 1 << 20);

        _reader = new WalReader(path, Path.Combine(options.DataDirectory, "checkpoint.bin"));

        _queue = new GroupCommitQueue(CommitAsync, TimeSpan.Zero);
    }

    public Task AcceptAsync(DemoMessage message, CancellationToken ct) => _queue.EnqueueAsync(message, ct);

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(() => _queue.RunAsync(stoppingToken), stoppingToken);

    private Task CommitAsync(IReadOnlyList<DemoMessage> batch)
    {
        foreach (var message in batch)
        {
            _log.Write(WalRecord.Encode(message));
        }

        _log.Flush(flushToDisk: true);

        Volatile.Write(ref _durableLength, _log.Length);

        return Task.CompletedTask;
    }

    // The relay only reads what fsync already made durable
    public Task<IReadOnlyList<DemoMessage>> ReadBatchAsync(CancellationToken ct)
    {
        return Task.FromResult(_reader.ReadBatch(Volatile.Read(ref _durableLength)));
    }

    public Task AcknowledgeAsync(CancellationToken ct)
    {
        _reader.Acknowledge();

        return Task.CompletedTask;
    }

    public override void Dispose()
    {
        _log.Dispose();

        _reader.Dispose();

        base.Dispose();
    }
}

/// <summary>docs/immediate-reply-periodic-fsync.md: replies right after write(), fsync runs every 10ms. Can lose accepted messages on power loss.</summary>
public sealed class ImmediateWal : BackgroundService, IMessageAcceptor, IRelaySource
{
    private readonly SafeFileHandle _log;

    private readonly WalReader _reader;

    private readonly Lock _writeLock = new();

    private long _length;

    public ImmediateWal(DemoOptions options)
    {
        var path = Path.Combine(options.DataDirectory, "wal.log");

        _log = File.OpenHandle(path, FileMode.Create, FileAccess.Write, FileShare.Read | FileShare.Delete);

        _reader = new WalReader(path, Path.Combine(options.DataDirectory, "checkpoint.bin"));
    }

    public Task AcceptAsync(DemoMessage message, CancellationToken ct)
    {
        var record = WalRecord.Encode(message);

        lock (_writeLock)
        {
            RandomAccess.Write(_log, record, _length);

            Volatile.Write(ref _length, _length + record.Length);
        }

        return Task.CompletedTask;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(10));

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                RandomAccess.FlushToDisk(_log);
            }
        }
        catch (OperationCanceledException)
        {
            RandomAccess.FlushToDisk(_log);
        }
    }

    // The relay does not wait for fsync: what reached Kafka survives a power loss
    public Task<IReadOnlyList<DemoMessage>> ReadBatchAsync(CancellationToken ct)
    {
        return Task.FromResult(_reader.ReadBatch(Volatile.Read(ref _length)));
    }

    public Task AcknowledgeAsync(CancellationToken ct)
    {
        _reader.Acknowledge();

        return Task.CompletedTask;
    }

    public override void Dispose()
    {
        _log.Dispose();

        _reader.Dispose();

        base.Dispose();
    }
}
