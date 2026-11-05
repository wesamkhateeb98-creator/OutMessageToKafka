using System.Globalization;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace DemoApi.Benchmarks;

public static class DemoEnvironment
{
    public const string KafkaServers = "localhost:19092,localhost:19093,localhost:19094";

    public static string DemoRoot { get; } = FindDemoRoot();

    // Run data stays on this path's disk, so the disk being measured is a choice, not an accident
    public static string DataRoot { get; } = Environment.GetEnvironmentVariable("DEMO_DATA_DIR")
        ?? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".cache", "outmessage-demo");

    public static async Task CreateTopicAsync(string topic)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = KafkaServers }).Build();

        await admin.CreateTopicsAsync(
        [
            new TopicSpecification
            {
                Name = topic,
                NumPartitions = 6,
                ReplicationFactor = 3,
                Configs = new Dictionary<string, string> { ["min.insync.replicas"] = "2" },
            },
        ]);
    }

    private static string FindDemoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);

        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "docker-compose.yml")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? throw new DirectoryNotFoundException("docker-compose.yml was not found above the test output.");
    }
}

public sealed record BenchmarkResult(
    string Solution,
    int Rate,
    int Concurrency,
    int Sent,
    int Accepted,
    int Errors,
    int Missing,
    int Duplicates,
    TimeSpan LastResponse,
    TimeSpan Delivered,
    double P50Ms,
    double P95Ms,
    double P99Ms,
    double ApiCpuPercentOfOneCore,
    double ApiPeakMemoryMb,
    double GeneratorCpuPercentOfMachine)
{
    private const string Header = "utc,solution,rate,concurrency,sent,accepted,errors,missing,duplicates,last_response_ms,delivered_ms,p50_ms,p95_ms,p99_ms,api_cpu_pct_core,api_peak_mb,generator_cpu_pct_machine";

    public static void Append(BenchmarkResult result)
    {
        var path = Path.Combine(DemoEnvironment.DemoRoot, "results", "benchmark.csv");

        Directory.CreateDirectory(Path.GetDirectoryName(path)!);

        if (!File.Exists(path))
        {
            File.WriteAllText(path, Header + Environment.NewLine);
        }

        File.AppendAllText(path, result.ToCsv() + Environment.NewLine);
    }

    private string ToCsv() => string.Join(',',
        DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture),
        Solution,
        Rate,
        Concurrency,
        Sent,
        Accepted,
        Errors,
        Missing,
        Duplicates,
        Format(LastResponse.TotalMilliseconds),
        Format(Delivered.TotalMilliseconds),
        Format(P50Ms),
        Format(P95Ms),
        Format(P99Ms),
        Format(ApiCpuPercentOfOneCore),
        Format(ApiPeakMemoryMb),
        Format(GeneratorCpuPercentOfMachine));

    private static string Format(double value) => value.ToString("F1", CultureInfo.InvariantCulture);
}
