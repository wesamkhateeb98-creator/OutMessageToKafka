using System.Diagnostics;

namespace DemoApi.Benchmarks;

/// <summary>Runs the Release build of DemoApi in its own process, as two instances behind Nginx for gateway-wal.</summary>
public sealed class ApiInstances : IAsyncDisposable
{
    private const string GatewayUrl = "http://127.0.0.1:18088/messages";

    private readonly List<Process> _processes;

    private readonly string _runDirectory;

    private readonly Dictionary<int, long> _peakMemory = [];

    private ApiInstances(List<Process> processes, string runDirectory, string url)
    {
        _processes = processes;

        _runDirectory = runDirectory;

        Url = url;
    }

    public string Url { get; }

    public static async Task<ApiInstances> StartAsync(string solution, string topic)
    {
        var dll = Path.Combine(DemoEnvironment.DemoRoot, "DemoApi", "bin", "Release", "net10.0", "DemoApi.dll");

        if (!File.Exists(dll))
        {
            throw new FileNotFoundException("Run with: dotnet test -c Release", dll);
        }

        var runDirectory = Path.Combine(DemoEnvironment.DataRoot, topic);

        var ports = solution == "gateway-wal" ? new[] { 15101, 15102 } : [15101];

        var processes = ports.Select((port, index) => Start(dll, solution, topic, Path.Combine(runDirectory, $"api-{index + 1}"), port)).ToList();

        foreach (var port in ports)
        {
            await WaitUntilHealthyAsync(port);
        }

        var url = solution == "gateway-wal" ? GatewayUrl : $"http://127.0.0.1:{ports[0]}/messages";

        return new ApiInstances(processes, runDirectory, url);
    }

    private static Process Start(string dll, string solution, string topic, string dataDirectory, int port)
    {
        var info = new ProcessStartInfo("dotnet")
        {
            UseShellExecute = false,
            WorkingDirectory = Path.GetDirectoryName(dll)!,
        };

        info.ArgumentList.Add(dll);

        info.ArgumentList.Add($"--urls=http://127.0.0.1:{port}");

        info.ArgumentList.Add($"--Demo:Solution={solution}");

        info.ArgumentList.Add($"--Demo:Topic={topic}");

        info.ArgumentList.Add($"--Demo:DataDirectory={dataDirectory}");

        return Process.Start(info) ?? throw new InvalidOperationException("DemoApi did not start.");
    }

    private static async Task WaitUntilHealthyAsync(int port)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };

        for (var attempt = 0; attempt < 100; attempt++)
        {
            try
            {
                using var response = await client.GetAsync($"http://127.0.0.1:{port}/health");

                if (response.IsSuccessStatusCode)
                {
                    return;
                }
            }
            catch (Exception exception) when (exception is HttpRequestException or TaskCanceledException)
            {
                // Still starting
            }

            await Task.Delay(100);
        }

        throw new TimeoutException($"DemoApi on port {port} did not become healthy.");
    }

    public TimeSpan CpuTime()
    {
        return _processes.Aggregate(TimeSpan.Zero, (total, process) =>
        {
            process.Refresh();

            return total + process.TotalProcessorTime;
        });
    }

    public long PeakMemoryBytes()
    {
        foreach (var process in _processes)
        {
            process.Refresh();

            _peakMemory[process.Id] = process.PeakWorkingSet64;
        }

        return _peakMemory.Values.Sum();
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var process in _processes)
        {
            process.Kill(entireProcessTree: true);

            await process.WaitForExitAsync();

            process.Dispose();
        }

        // Filebeat may still hold a deleted log open, which is harmless
        if (Directory.Exists(_runDirectory))
        {
            Directory.Delete(_runDirectory, recursive: true);
        }
    }
}
