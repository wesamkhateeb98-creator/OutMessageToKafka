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
    double AppCpuPercentOfOneCore,
    double AppPeakMemoryMb)
{
    // The CSV shows names, because s1..s6 only mean something next to Program.cs
    public static readonly IReadOnlyDictionary<string, string> SolutionNames = new Dictionary<string, string>
    {
        ["s1"] = "await",
        ["s2"] = "blob",
        ["s3"] = "local",
        ["s4"] = "outbox",
        ["s5"] = "sqlite",
        ["s6"] = "zlogger",
    };

    private const string Header = "utc,solution,rate,concurrency,message_bytes,sent,accepted,errors,missing,duplicates,load_ms,delivered_ms,p50_ms,p95_ms,p99_ms,app_cpu_pct_core,app_peak_mb";

    public static void Append(BenchmarkResult result)
    {
        var path = Path.Combine(DemoEnvironment.DemoRoot, "DemoApi.Benchmarks", "results", "benchmark-1kb.csv");

        Directory.CreateDirectory(Path.GetDirectoryName(path)!);

        if (!File.Exists(path))
        {
            File.WriteAllText(path, Header + Environment.NewLine);
        }

        File.AppendAllText(path, result.ToCsv() + Environment.NewLine);

        SortBySolution(path);

        WriteMedians(path, Path.Combine(Path.GetDirectoryName(path)!, "benchmark-1kb-median.csv"));
    }

    /// <summary>
    /// One row per solution. For each value: the median of the rounds at each load, so one odd round cannot move it,
    /// then the mean of those medians.
    /// </summary>
    public static void WriteMedians(string resultsPath, string mediansPath)
    {
        var lines = File.ReadAllLines(resultsPath);

        var column = lines[0].Split(',').Select((name, index) => (name, index)).ToDictionary(c => c.name, c => c.index);

        var rows = lines.Skip(1).Select(line => line.Split(',')).Where(IsRound).ToList();

        double Value(string[] row, string name) => double.Parse(row[column[name]], CultureInfo.InvariantCulture);

        var metrics = new (string Name, Func<string[], double> Read, string Format)[]
        {
            ("p50_ms", row => Value(row, "p50_ms"), "F1"),
            ("p95_ms", row => Value(row, "p95_ms"), "F1"),
            ("p99_ms", row => Value(row, "p99_ms"), "F1"),
            ("accepted_per_s", row => Value(row, "accepted") * 1000 / Value(row, "load_ms"), "F0"),
            ("delivered_per_s", row => Value(row, "accepted") * 1000 / Value(row, "delivered_ms"), "F0"),
        };

        var rates = rows.Select(row => (int)Value(row, "rate")).Distinct().OrderByDescending(rate => rate).ToList();

        var header = new List<string> { "solution", "rounds", "errors", "missing", "duplicates" };

        foreach (var rate in rates)
        {
            var load = rate == 0 ? "max" : rate.ToString(CultureInfo.InvariantCulture);

            header.AddRange(metrics.Select(metric => $"{metric.Name}_{load}"));
        }

        header.AddRange(metrics.Select(metric => $"{metric.Name}_mean"));

        var output = new List<string> { string.Join(',', header) };

        foreach (var solution in SolutionNames.Values)
        {
            var mine = rows.Where(row => row[column["solution"]] == solution).ToList();

            if (mine.Count == 0)
            {
                continue;
            }

            var cells = new List<string>
            {
                solution,
                mine.Count.ToString(CultureInfo.InvariantCulture),
                Total(mine, "errors"),
                Total(mine, "missing"),
                Total(mine, "duplicates"),
            };

            // medians[rate index][metric index], kept as numbers for the mean columns
            var medians = rates
                .Select(rate => mine.Where(row => (int)Value(row, "rate") == rate).ToList())
                .Select(atRate => metrics.Select(metric => MedianValue(atRate.Select(metric.Read))).ToArray())
                .ToList();

            foreach (var atRate in medians)
            {
                cells.AddRange(atRate.Select((median, index) => Format(median, metrics[index].Format)));
            }

            cells.AddRange(metrics.Select((metric, index) => Format(medians.Average(atRate => atRate[index]), metric.Format)));

            output.Add(string.Join(',', cells));
        }

        File.WriteAllLines(mediansPath, output);

        string Total(List<string[]> group, string name) => group.Sum(row => (int)Value(row, name)).ToString(CultureInfo.InvariantCulture);
    }

    public static string Median(IEnumerable<double> values, string format) => Format(MedianValue(values), format);

    // Sorted middle value; with an even count, the mean of the two middle values. NaN when there is nothing to sort
    private static double MedianValue(IEnumerable<double> values)
    {
        var sorted = values.Order().ToArray();

        if (sorted.Length == 0)
        {
            return double.NaN;
        }

        var middle = sorted.Length / 2;

        return sorted.Length % 2 == 1 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
    }

    // A missing load leaves an empty cell, never "NaN"
    private static string Format(double value, string format) =>
        double.IsNaN(value) ? string.Empty : value.ToString(format, CultureInfo.InvariantCulture);

    // xUnit runs the cases in any order, so the file is sorted again after each row: solution, then rate 5000 before 0,
    // then time. An empty row separates solutions, so the file reads as one block per solution
    private static void SortBySolution(string path)
    {
        var order = SolutionNames.Values.ToList();

        var rows = File.ReadAllLines(path).Skip(1).Select(line => line.Split(',')).Where(IsRound);

        var blocks = rows
            .OrderBy(row => order.IndexOf(row[1]))
            .ThenByDescending(row => int.Parse(row[2], CultureInfo.InvariantCulture))
            .ThenBy(row => row[0], StringComparer.Ordinal)
            .GroupBy(row => row[1])
            .Select(block => block.Select(row => string.Join(',', row)));

        var separator = new string(',', Header.Count(character => character == ','));

        var lines = blocks.SelectMany((block, index) => index == 0 ? block : block.Prepend(separator));

        File.WriteAllLines(path, lines.Prepend(Header));
    }

    // Skips the empty separator rows, which have no solution
    private static bool IsRound(string[] row) => row.Length > 1 && row[1].Length > 0;

    private string ToCsv() => string.Join(',',
        DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture),
        SolutionNames[Solution],
        Rate,
        Concurrency,
        LoadRunner.MessageBytes,
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
        Format(AppCpuPercentOfOneCore),
        Format(AppPeakMemoryMb));

    private static string Format(double value) => value.ToString("F1", CultureInfo.InvariantCulture);
}
