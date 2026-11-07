using System.Diagnostics;
using Xunit;
using Xunit.Abstractions;

// A second test class running at the same time would share the port and skew the numbers
[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace DemoApi.Benchmarks;

/// <summary>
/// Runs every solution at every load for every round: the app sends DEMO_COUNT messages of 1KB (default 10,000) to
/// itself, then this test checks that every accepted message reached Kafka. Rate 5,000 is an arrival rate per second;
/// rate 0 sends as fast as DEMO_CONCURRENCY calls at once allow (default 256).
/// </summary>
public sealed class SolutionBenchmarkTests(ITestOutputHelper output)
{
    private const int WarmupCount = 2_000;

    private static readonly TimeSpan LoadTimeout = TimeSpan.FromMinutes(10);

    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromMinutes(3);

    public static TheoryData<string, int, int> AllCases()
    {
        var cases = new TheoryData<string, int, int>();

        foreach (var solution in new[] { "s1", "s2", "s3", "s4", "s5", "s6" })
        {
            foreach (var rate in new[] { 5_000, 0 })
            {
                foreach (var round in new[] { 1, 2, 3 })
                {
                    cases.Add(solution, rate, round);
                }
            }
        }

        return cases;
    }

    [Theory]
    [MemberData(nameof(AllCases))]
    public async Task EveryAcceptedMessageReachesKafka(string solution, int rate, int round)
    {
        var count = EnvironmentInt("DEMO_COUNT", 10_000);

        var concurrency = EnvironmentInt("DEMO_CONCURRENCY", 256);

        var topic = $"demo-{solution}-r{rate}-{round}-{DateTime.UtcNow:yyyyMMddHHmmss}";

        await DemoEnvironment.CreateTopicAsync(topic);

        using var consumer = new TopicConsumer(DemoEnvironment.KafkaServers, topic);

        await using var app = AppProcess.Start(solution, topic, new LoadSettings(count, rate, concurrency, WarmupCount));

        var measured = await app.WaitForResultAsync(LoadTimeout);

        var acceptedIds = app.ReadAcceptedIds();

        var missing = await consumer.WaitForAsync(acceptedIds, DeliveryTimeout);

        var delivered = consumer.LastNewMessageAtUtc - measured.StartedAtUtc;

        await consumer.WaitForTrailingDuplicatesAsync();

        var appCpu = app.CpuTime() - TimeSpan.FromMilliseconds(measured.CpuAtStartMs);

        var result = new BenchmarkResult(
            solution,
            rate,
            concurrency,
            count,
            measured.Accepted,
            measured.Errors,
            missing,
            consumer.Duplicates,
            TimeSpan.FromMilliseconds(measured.ElapsedMs),
            delivered,
            measured.P50Ms,
            measured.P95Ms,
            measured.P99Ms,
            appCpu / delivered * 100,
            app.PeakMemoryBytes() / (1024.0 * 1024.0));

        // The row is saved first, so a failed assertion still leaves its numbers
        BenchmarkResult.Append(result);

        output.WriteLine(result.ToString());

        Assert.Equal(0, missing);

        Assert.Equal(0, measured.Errors);
    }

    private static int EnvironmentInt(string name, int fallback)
    {
        var value = Environment.GetEnvironmentVariable(name);

        return string.IsNullOrEmpty(value) ? fallback : int.Parse(value);
    }
}
