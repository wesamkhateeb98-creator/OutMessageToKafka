using System.Buffers;
using System.Buffers.Text;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;

namespace DemoApi;

/// <summary>docs/zlogger-file-relay.md: the file ZLogger writes to. A waiter completes only after fsync covered its whole line.</summary>
public sealed class DurableLogStream : Stream
{
    private const int ReadOnlyFlag = 0;

    private readonly string _directory;

    private readonly long _maxFileBytes;

    private readonly Action<FileStream> _syncFile;

    private readonly string _runId = DateTime.UtcNow.ToString("yyyyMMddHHmmssfff");

    private readonly ConcurrentDictionary<long, TaskCompletionSource> _waiters = new();

    private readonly List<long> _unsyncedSeqs = [];

    private readonly ArrayBufferWriter<byte> _partialLine = new();

    private volatile Exception? _failure;

    private FileStream _file;

    private long _fileLength;

    private int _fileNumber;

    public DurableLogStream(string directory, long maxFileBytes, Action<FileStream> syncFile)
    {
        _directory = directory;

        _maxFileBytes = maxFileBytes;

        _syncFile = syncFile;

        _file = OpenNextFile();
    }

    private static ReadOnlySpan<byte> SeqPrefix => "{\"seq\":"u8;

    public override bool CanRead => false;

    public override bool CanSeek => false;

    public override bool CanWrite => true;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();

        set => throw new NotSupportedException();
    }

    public static void SyncToDisk(FileStream file) => file.Flush(flushToDisk: true);

    public Task RegisterAsync(long seq)
    {
        var waiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        _waiters[seq] = waiter;

        var failure = _failure;

        // Fail may have swept the waiters just before this one was added
        if (failure is not null && _waiters.TryRemove(seq, out _))
        {
            waiter.TrySetException(failure);
        }

        return waiter.Task;
    }

    public void Forget(long seq) => _waiters.TryRemove(seq, out _);

    public override void Write(byte[] buffer, int offset, int count)
    {
        if (_failure is not null)
        {
            return;
        }

        try
        {
            _file.Write(buffer, offset, count);

            _fileLength += count;

            CollectFinishedLines(buffer.AsSpan(offset, count));
        }
        catch (Exception error)
        {
            Fail(error);
        }
    }

    // ZLogger swallows what this method throws and keeps looping, so a failure is handled here
    public override void Flush()
    {
        if (_failure is not null)
        {
            return;
        }

        try
        {
            _syncFile(_file);

            CompleteSyncedWaiters();

            RotateIfFull();
        }
        catch (Exception error)
        {
            Fail(error);
        }
    }

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Fail(new ObjectDisposedException(nameof(DurableLogStream)));

            _file.Dispose();
        }

        base.Dispose(disposing);
    }

    // ZLogger may split a line between two writes, and only a finished line can be acknowledged
    private void CollectFinishedLines(ReadOnlySpan<byte> written)
    {
        var lineEnd = written.IndexOf((byte)'\n');

        while (lineEnd >= 0)
        {
            var line = written[..lineEnd];

            if (_partialLine.WrittenCount > 0)
            {
                _partialLine.Write(line);

                line = _partialLine.WrittenSpan;
            }

            if (line.StartsWith(SeqPrefix) && Utf8Parser.TryParse(line[SeqPrefix.Length..], out long seq, out _))
            {
                _unsyncedSeqs.Add(seq);
            }

            _partialLine.ResetWrittenCount();

            written = written[(lineEnd + 1)..];

            lineEnd = written.IndexOf((byte)'\n');
        }

        _partialLine.Write(written);
    }

    private void CompleteSyncedWaiters()
    {
        foreach (var seq in _unsyncedSeqs)
        {
            if (_waiters.TryRemove(seq, out var waiter))
            {
                waiter.TrySetResult();
            }
        }

        _unsyncedSeqs.Clear();
    }

    private void RotateIfFull()
    {
        if (_fileLength < _maxFileBytes || _partialLine.WrittenCount > 0)
        {
            return;
        }

        var next = OpenNextFile();

        _file.Dispose();

        _file = next;

        _fileLength = 0;
    }

    // A new name on every start: after a power loss, appending to an old file could end up behind Filebeat's saved position
    private FileStream OpenNextFile()
    {
        _fileNumber++;

        var path = Path.Combine(_directory, $"zlogger-{_runId}-{_fileNumber:D6}.log");

        var file = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.Read | FileShare.Delete, bufferSize: 0);

        SyncDirectory(_directory);

        return file;
    }

    private void Fail(Exception error)
    {
        _failure = error;

        foreach (var seq in _waiters.Keys)
        {
            if (_waiters.TryRemove(seq, out var waiter))
            {
                waiter.TrySetException(error);
            }
        }
    }

    // Without this, a power loss can remove a new file from its directory even after the file itself was synced
    private static void SyncDirectory(string directory)
    {
        if (!OperatingSystem.IsLinux())
        {
            return;
        }

        var descriptor = OpenDescriptor(directory, ReadOnlyFlag);

        if (descriptor < 0)
        {
            throw new IOException($"Could not open {directory}: {Marshal.GetLastPInvokeErrorMessage()}");
        }

        try
        {
            if (SyncDescriptor(descriptor) != 0)
            {
                throw new IOException($"Could not sync {directory}: {Marshal.GetLastPInvokeErrorMessage()}");
            }
        }
        finally
        {
            CloseDescriptor(descriptor);
        }
    }

    [DllImport("libc", EntryPoint = "open", SetLastError = true)]
    private static extern int OpenDescriptor(string path, int flags);

    [DllImport("libc", EntryPoint = "fsync", SetLastError = true)]
    private static extern int SyncDescriptor(int descriptor);

    [DllImport("libc", EntryPoint = "close")]
    private static extern int CloseDescriptor(int descriptor);
}
