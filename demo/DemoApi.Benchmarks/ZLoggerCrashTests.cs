using System.Text.RegularExpressions;
using Xunit;

namespace DemoApi.Benchmarks;

/// <summary>A process kill keeps the page cache, so this covers the memory stage only. It cannot prove fsync survives a power loss.</summary>
public sealed class ZLoggerCrashTests
{
    [Fact]
    public async Task Kill_DuringBurst_EveryAcceptedMessageIsInTheFile()
    {
        // Arrange
        var topic = $"demo-crash-{DateTime.UtcNow:yyyyMMddHHmmssfff}";

        await using var api = await ApiInstances.StartAsync("s6", topic);

        using var client = LoadGenerator.CreateClient();

        var burst = LoadGenerator.RunAsync(client, api.Url, count: 15_000, rate: 0, concurrency: 256);

        await Task.Delay(TimeSpan.FromSeconds(1));

        // Act
        api.Kill();

        var result = await burst;

        // Assert
        var fileText = string.Concat(Directory.GetFiles(api.RunDirectory, "zlogger-*.log", SearchOption.AllDirectories).Select(ReadShared));

        var idsInFile = Regex.Matches(fileText, "\"id\":\"([^\"]+)\"").Select(match => match.Groups[1].Value).ToHashSet();

        var missing = result.AcceptedIds.Count(id => !idsInFile.Contains(id));

        Assert.NotEmpty(result.AcceptedIds);

        Assert.True(result.Errors > 0, "The kill came after the burst finished, so it proved nothing.");

        Assert.Equal(0, missing);
    }

    private static string ReadShared(string path)
    {
        using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);

        using var reader = new StreamReader(file);

        return reader.ReadToEnd();
    }
}
