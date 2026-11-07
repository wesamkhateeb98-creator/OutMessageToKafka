using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;

namespace DemoApi.Benchmarks;

public sealed record LoadResult(IReadOnlyCollection<string> AcceptedIds, int Errors, long StartedAt, TimeSpan Elapsed, double[] SortedLatenciesMs)
{
    public double Percentile(double quantile)
    {
        if (SortedLatenciesMs.Length == 0)
        {
            return double.NaN;
        }

        return SortedLatenciesMs[Math.Min(SortedLatenciesMs.Length - 1, (int)(SortedLatenciesMs.Length * quantile))];
    }
}

public static class LoadGenerator
{
    public const int MessageBytes = 1024;

    // ASCII only, so every character is one byte and each body is exactly MessageBytes
    public static string BuildBody(int index)
    {
        var email = $"user-{index}@example.com";

        var name = $"user-{index}-";

        var empty = $$"""{"name":"{{name}}","email":"{{email}}"}""";

        var padding = new string('x', MessageBytes - empty.Length);

        return $$"""{"name":"{{name}}{{padding}}","email":"{{email}}"}""";
    }

    public static HttpClient CreateClient()
    {
        var handler = new SocketsHttpHandler
        {
            MaxConnectionsPerServer = 1024,
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(10),
        };

        return new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(30) };
    }

    public static async Task<LoadResult> RunAsync(HttpClient client, string url, int count, int rate, int concurrency)
    {
        var accepted = new ConcurrentBag<string>();

        var latencies = new double[count];

        var errors = 0;

        var startedAt = Stopwatch.GetTimestamp();

        // A latency starts at the scheduled time, so a slow server cannot hide its own queue
        async Task SendAsync(int index, long scheduledAt)
        {
            var body = BuildBody(index);

            try
            {
                using var content = new StringContent(body, Encoding.UTF8, "application/json");

                using var response = await client.PostAsync(url, content);

                if (response.IsSuccessStatusCode && response.Headers.TryGetValues("X-Message-Id", out var ids))
                {
                    accepted.Add(ids.First());
                }
                else
                {
                    Interlocked.Increment(ref errors);
                }
            }
            catch (Exception)
            {
                Interlocked.Increment(ref errors);
            }

            latencies[index] = Stopwatch.GetElapsedTime(scheduledAt).TotalMilliseconds;
        }

        if (rate > 0)
        {
            await SendAtRateAsync(SendAsync, count, rate, startedAt);
        }
        else
        {
            await SendWithConcurrencyAsync(SendAsync, count, concurrency);
        }

        var elapsed = Stopwatch.GetElapsedTime(startedAt);

        Array.Sort(latencies);

        return new LoadResult(accepted, errors, startedAt, elapsed, latencies);
    }

    // Open model: requests arrive on schedule, whether the server is slow or not
    private static async Task SendAtRateAsync(Func<int, long, Task> sendAsync, int count, int rate, long startedAt)
    {
        var tasks = new Task[count];

        var sent = 0;

        while (sent < count)
        {
            var due = Math.Min(count, (int)(Stopwatch.GetElapsedTime(startedAt).TotalSeconds * rate) + 1);

            for (; sent < due; sent++)
            {
                var scheduledAt = startedAt + (long)(sent / (double)rate * Stopwatch.Frequency);

                tasks[sent] = StartAsync(sendAsync, sent, scheduledAt);
            }

            await Task.Delay(1);
        }

        await Task.WhenAll(tasks);
    }

    private static Task StartAsync(Func<int, long, Task> sendAsync, int index, long scheduledAt)
    {
        return Task.Run(() => sendAsync(index, scheduledAt));
    }

    // Closed model: answers "how long do N requests take" at full speed
    private static async Task SendWithConcurrencyAsync(Func<int, long, Task> sendAsync, int count, int concurrency)
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

                await sendAsync(index, Stopwatch.GetTimestamp());
            }
        }));

        await Task.WhenAll(workers);
    }
}
