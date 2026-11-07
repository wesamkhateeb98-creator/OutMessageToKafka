using System.Diagnostics;
using Xunit;
using Xunit.Abstractions;

// A second test class running at the same time would share the port and skew the numbers
[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace DemoApi.Benchmarks;

/// <summary>
/// Sends DEMO_COUNT requests (default 10,000) of 1KB to every solution, at every load, for every round, then checks
/// that every accepted message reached Kafka. Rate 5,000 is an arrival rate per second; rate 0 sends as fast as
/// DEMO_CONCURRENCY open requests allow (default 256).
/// </summary>
public sealed class SolutionBenchmarkTests(ITestOutputHelper output)
{
    private const int WarmupCount = 2_000;

    private const int WarmupRate = 500;

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

        await using var api = await ApiInstances.StartAsync(solution, topic);

        using var client = LoadGenerator.CreateClient();

        var warmup = await LoadGenerator.RunAsync(client, api.Url, WarmupCount, WarmupRate, concurrency);

        await consumer.WaitForAsync(warmup.AcceptedIds, DeliveryTimeout);

        var generatorCpuBefore = Process.GetCurrentProcess().TotalProcessorTime;

        var apiCpuBefore = api.CpuTime();

        var measured = await LoadGenerator.RunAsync(client, api.Url, count, rate, concurrency);

        var missing = await consumer.WaitForAsync(measured.AcceptedIds, DeliveryTimeout);

        var delivered = Stopwatch.GetElapsedTime(measured.StartedAt, consumer.LastNewMessageAt);

        await consumer.WaitForTrailingDuplicatesAsync();

        var result = new BenchmarkResult(
            solution,
            rate,
            concurrency,
            count,
            measured.AcceptedIds.Count,
            measured.Errors,
            missing,
            consumer.Duplicates,
            measured.Elapsed,
            delivered,
            measured.Percentile(0.50),
            measured.Percentile(0.95),
            measured.Percentile(0.99),
            (api.CpuTime() - apiCpuBefore) / delivered * 100,
            api.PeakMemoryBytes() / (1024.0 * 1024.0),
            (Process.GetCurrentProcess().TotalProcessorTime - generatorCpuBefore) / delivered / Environment.ProcessorCount * 100);

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
