using System.Text;
using Xunit;

namespace DemoApi.Benchmarks;

public sealed class DurableLogStreamTests : IDisposable
{
    private readonly string _directory = Directory.CreateTempSubdirectory("durable-log-").FullName;

    [Fact]
    public void Write_LineNotSynced_WaiterStaysPending()
    {
        // Arrange
        using var stream = NewStream();

        var waiter = stream.RegisterAsync(7);

        // Act
        WriteText(stream, Line(7));

        // Assert
        Assert.False(waiter.IsCompleted);
    }

    [Fact]
    public async Task Flush_LineWritten_CompletesItsWaiterOnly()
    {
        // Arrange
        using var stream = NewStream();

        var written = stream.RegisterAsync(7);

        var notWritten = stream.RegisterAsync(8);

        WriteText(stream, Line(7));

        // Act
        stream.Flush();

        await written.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        Assert.True(written.IsCompletedSuccessfully);

        Assert.False(notWritten.IsCompleted);
    }

    [Fact]
    public async Task Flush_LineSplitAcrossWrites_CompletesOnlyAfterItsSecondPart()
    {
        // Arrange
        using var stream = NewStream();

        var waiter = stream.RegisterAsync(7);

        var line = Line(7);

        WriteText(stream, line[..10]);

        // Act
        stream.Flush();

        var pendingAfterFirstPart = !waiter.IsCompleted;

        WriteText(stream, line[10..]);

        stream.Flush();

        await waiter.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        Assert.True(pendingAfterFirstPart);

        Assert.True(waiter.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task Flush_SyncFails_FailsWaitersAndLaterRegistrations()
    {
        // Arrange
        using var stream = new DurableLogStream(_directory, 1L << 30, _ => throw new IOException("Disk failed."));

        var waiter = stream.RegisterAsync(7);

        WriteText(stream, Line(7));

        // Act
        stream.Flush();

        var later = stream.RegisterAsync(8);

        // Assert
        await Assert.ThrowsAsync<IOException>(() => waiter);

        await Assert.ThrowsAsync<IOException>(() => later);
    }

    [Fact]
    public void Flush_FileReachedLimit_NextLineGoesToNewFile()
    {
        // Arrange
        using var stream = new DurableLogStream(_directory, 1, DurableLogStream.SyncToDisk);

        WriteText(stream, Line(7));

        // Act
        stream.Flush();

        WriteText(stream, Line(8));

        stream.Flush();

        // Assert
        var files = Directory.GetFiles(_directory, "zlogger-*.log").Order().ToArray();

        Assert.Equal(3, files.Length);

        Assert.Equal(Line(7), ReadShared(files[0]));

        Assert.Equal(Line(8), ReadShared(files[1]));

        Assert.Equal(string.Empty, ReadShared(files[2]));
    }

    public void Dispose() => Directory.Delete(_directory, recursive: true);

    private DurableLogStream NewStream() => new(_directory, 1L << 30, DurableLogStream.SyncToDisk);

    private static string Line(long seq) => $$"""{"seq":{{seq}},"id":"a"}""" + "\n";

    private static void WriteText(Stream stream, string text)
    {
        var bytes = Encoding.UTF8.GetBytes(text);

        stream.Write(bytes, 0, bytes.Length);
    }

    private static string ReadShared(string path)
    {
        using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

        using var reader = new StreamReader(file);

        return reader.ReadToEnd();
    }
}
