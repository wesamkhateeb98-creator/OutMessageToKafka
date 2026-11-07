using System.Text.RegularExpressions;
using Xunit;

namespace DemoApi.Benchmarks;

/// <summary>A process kill keeps the page cache, so this covers the memory stage only. It cannot prove fsync survives a power loss.</summary>
public sealed class ZLoggerCrashTests
{
    private const int Count = 15_000;

    [Fact]
    public async Task Kill_DuringBurst_EveryAcceptedMessageIsInTheFile()
    {
        // Arrange
        var topic = $"demo-crash-{DateTime.UtcNow:yyyyMMddHHmmssfff}";

        await using var app = AppProcess.Start("s6", topic, new LoadSettings(Count, Rate: 0, Concurrency: 256, Warmup: 0));

        await app.WaitForAcceptedAsync(minimum: 1_000, TimeSpan.FromSeconds(30));

        // Act
        app.Kill();

        var acceptedIds = app.ReadAcceptedIds();

        // Assert
        var fileText = string.Concat(Directory.GetFiles(app.RunDirectory, "zlogger-*.log", SearchOption.AllDirectories).Select(ReadShared));

        var idsInFile = Regex.Matches(fileText, "\"id\":\"([^\"]+)\"").Select(match => match.Groups[1].Value).ToHashSet();

        var missing = acceptedIds.Count(id => !idsInFile.Contains(id));

        Assert.True(acceptedIds.Count < Count, "The kill came after the burst finished, so it proved nothing.");

        Assert.Equal(0, missing);
    }

    private static string ReadShared(string path)
    {
        using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);

        using var reader = new StreamReader(file);

        return reader.ReadToEnd();
    }
}
