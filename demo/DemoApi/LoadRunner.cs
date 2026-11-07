using System.Diagnostics;
using System.Globalization;
using System.Text.Json;

namespace DemoApi;

/// <summary>The Load section of appsettings.json. Rate 0 sends as fast as Concurrency allows; a higher rate sends on a fixed schedule.</summary>
public sealed record LoadSettings(int Count, int Rate, int Concurrency, int Warmup)
{
    // Lets the command line say --count=100000 instead of --Load:Count=100000
    public static readonly Dictionary<string, string> SwitchMappings = new()
    {
        ["--count"] = "Load:Count",
        ["--rate"] = "Load:Rate",
        ["--concurrency"] = "Load:Concurrency",
        ["--warmup"] = "Load:Warmup",
    };
}

/// <summary>What one measured run produced. Saved as result.json so the benchmark process can read it.</summary>
public sealed record LoadResult(
    int Sent,
    int Accepted,
    int Errors,
    DateTime StartedAtUtc,
    double ElapsedMs,
    double P50Ms,
    double P95Ms,
    double P99Ms,
    double CpuAtStartMs)
{
    public const string FileName = "result.json";

    public double PerSecond => Accepted / (ElapsedMs / 1000);

    public string Describe(string solution) => string.Create(CultureInfo.InvariantCulture, $"""
        {solution}  sent={Sent:N0}  accepted={Accepted:N0}  errors={Errors:N0}
            p50={P50Ms:F1}ms  p95={P95Ms:F1}ms  p99={P99Ms:F1}ms
            throughput={PerSecond:N0}/s  {Sent:N0} in {ElapsedMs / 1000:F1}s
        """);

    // Written to a temporary name first, so a reader never sees half a file
    public void Save(string directory)
    {
        var path = Path.Combine(directory, FileName);

        File.WriteAllText(path + ".tmp", JsonSerializer.Serialize(this));

        File.Move(path + ".tmp", path, overwrite: true);
    }

    public static LoadResult Read(string path) => JsonSerializer.Deserialize<LoadResult>(File.ReadAllText(path))!;
}

/// <summary>Stands in for the clients: builds 1KB messages and calls the solution directly, as many at once as the settings allow.</summary>
public static class LoadRunner
{
    public const int MessageBytes = 1024;

    public const string AcceptedIdsFileName = "accepted-ids.txt";

    private const int WarmupRate = 500;

    // ASCII only, so {"name":...,"email":...} is exactly MessageBytes for every index
    public static DemoMessage BuildMessage(int index)
    {
        var email = $"user-{index}@example.com";

        var name = $"user-{index}-";

        var empty = $$"""{"name":"{{name}}","email":"{{email}}"}""";

        var padding = new string('x', MessageBytes - empty.Length);

        return new DemoMessage(Guid.CreateVersion7().ToString("N"), name + padding, email);
    }

    public static async Task<LoadResult> RunAsync(IMessageAcceptor acceptor, LoadSettings load, string directory, CancellationToken ct)
    {
        // The warmup lets the JIT and connections settle, and its ids are not part of the result
        if (load.Warmup > 0)
        {
            await SendAsync(acceptor, load.Warmup, WarmupRate, load.Concurrency, acceptedLog: null, ct);
        }

        // Each id is flushed as soon as it is accepted, so a killed process still leaves the ids it promised
        await using var acceptedLog = new StreamWriter(Path.Combine(directory, AcceptedIdsFileName)) { AutoFlush = true };

        var cpuAtStart = Process.GetCurrentProcess().TotalProcessorTime;

        var startedAtUtc = DateTime.UtcNow;

        var run = await SendAsync(acceptor, load.Count, load.Rate, load.Concurrency, acceptedLog, ct);

        return new LoadResult(
            load.Count,
            run.Accepted,
            run.Errors,
            startedAtUtc,
            run.Elapsed.TotalMilliseconds,
            Percentile(run.SortedLatenciesMs, 0.50),
            Percentile(run.SortedLatenciesMs, 0.95),
            Percentile(run.SortedLatenciesMs, 0.99),
            cpuAtStart.TotalMilliseconds);
    }

    private static async Task<RunStats> SendAsync(IMessageAcceptor acceptor, int count, int rate, int concurrency, StreamWriter? acceptedLog, CancellationToken ct)
    {
        var latencies = new double[count];

        var accepted = 0;

        var errors = 0;

        var startedAt = Stopwatch.GetTimestamp();

        // A latency starts at the scheduled time, so a slow solution cannot hide its own queue
        async Task SendOneAsync(int index, long scheduledAt)
        {
            var message = BuildMessage(index);

            try
            {
                await acceptor.AcceptAsync(message, ct);

                Interlocked.Increment(ref accepted);

                if (acceptedLog is not null)
                {
                    lock (acceptedLog)
                    {
                        acceptedLog.WriteLine(message.Id);
                    }
                }
            }
            catch (Exception) when (!ct.IsCancellationRequested)
            {
                Interlocked.Increment(ref errors);
            }

            latencies[index] = Stopwatch.GetElapsedTime(scheduledAt).TotalMilliseconds;
        }

        if (rate > 0)
        {
            await SendAtRateAsync(SendOneAsync, count, rate, startedAt);
        }
        else
        {
            await SendWithConcurrencyAsync(SendOneAsync, count, concurrency);
        }

        var elapsed = Stopwatch.GetElapsedTime(startedAt);

        Array.Sort(latencies);

        return new RunStats(accepted, errors, elapsed, latencies);
    }

    // Open model: messages arrive on schedule, whether the solution is slow or not
    private static async Task SendAtRateAsync(Func<int, long, Task> sendOneAsync, int count, int rate, long startedAt)
    {
        var tasks = new Task[count];

        var sent = 0;

        while (sent < count)
        {
            var due = Math.Min(count, (int)(Stopwatch.GetElapsedTime(startedAt).TotalSeconds * rate) + 1);

            for (; sent < due; sent++)
            {
                var index = sent;

                var scheduledAt = startedAt + (long)(index / (double)rate * Stopwatch.Frequency);

                tasks[index] = Task.Run(() => sendOneAsync(index, scheduledAt));
            }

            await Task.Delay(1);
        }

        await Task.WhenAll(tasks);
    }

    // Closed model: answers "how long do N messages take" at full speed
    private static async Task SendWithConcurrencyAsync(Func<int, long, Task> sendOneAsync, int count, int concurrency)
    {
        var next = -1;

        var workers = Enumerable.Range(0, concurrency).Select(_ => Task.Run(async () =>
        {
            while (true)
            {
                var index = Interlocked.Increment(ref next);

                if (index >= count)
                {
                    return;
                }

                await sendOneAsync(index, Stopwatch.GetTimestamp());
            }
        }));

        await Task.WhenAll(workers);
    }

    private static double Percentile(double[] sorted, double quantile) =>
        sorted.Length == 0 ? double.NaN : sorted[Math.Min(sorted.Length - 1, (int)(sorted.Length * quantile))];

    private sealed record RunStats(int Accepted, int Errors, TimeSpan Elapsed, double[] SortedLatenciesMs);
}
