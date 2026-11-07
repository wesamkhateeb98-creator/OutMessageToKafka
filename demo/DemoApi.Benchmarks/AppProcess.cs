using System.Collections.Concurrent;
using System.Diagnostics;

namespace DemoApi.Benchmarks;

/// <summary>Runs the Release build of DemoApi in its own process. The app sends the load itself and writes result.json.</summary>
public sealed class AppProcess : IAsyncDisposable
{
    private readonly Process _process;

    private readonly string _runDirectory;

    private readonly string _dataDirectory;

    private readonly ConcurrentQueue<string> _output = new();

    private long _peakMemory;

    private AppProcess(Process process, string runDirectory, string dataDirectory)
    {
        _process = process;

        _runDirectory = runDirectory;

        _dataDirectory = dataDirectory;
    }

    public string RunDirectory => _runDirectory;

    public static AppProcess Start(string solution, string topic, LoadSettings load)
    {
        var dll = Path.Combine(DemoEnvironment.DemoRoot, "DemoApi", "bin", "Release", "net10.0", "DemoApi.dll");

        if (!File.Exists(dll))
        {
            throw new FileNotFoundException("Run with: dotnet test -c Release", dll);
        }

        // Filebeat reads /data/<topic>/<instance>/zlogger-*.log, so the data sits two levels under DataRoot
        var runDirectory = Path.Combine(DemoEnvironment.DataRoot, topic);

        var dataDirectory = Path.Combine(runDirectory, "app-1");

        var info = new ProcessStartInfo("dotnet")
        {
            UseShellExecute = false,
            WorkingDirectory = Path.GetDirectoryName(dll)!,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };

        info.ArgumentList.Add(dll);

        info.ArgumentList.Add($"--solution={solution}");

        info.ArgumentList.Add($"--Demo:Topic={topic}");

        info.ArgumentList.Add($"--Demo:DataDirectory={dataDirectory}");

        info.ArgumentList.Add($"--count={load.Count}");

        info.ArgumentList.Add($"--rate={load.Rate}");

        info.ArgumentList.Add($"--concurrency={load.Concurrency}");

        info.ArgumentList.Add($"--warmup={load.Warmup}");

        var process = Process.Start(info) ?? throw new InvalidOperationException("DemoApi did not start.");

        var app = new AppProcess(process, runDirectory, dataDirectory);

        // Read both streams, or a full pipe would block the app
        process.OutputDataReceived += (_, line) => app.Keep(line.Data);

        process.ErrorDataReceived += (_, line) => app.Keep(line.Data);

        process.BeginOutputReadLine();

        process.BeginErrorReadLine();

        return app;
    }

    public async Task<LoadResult> WaitForResultAsync(TimeSpan timeout)
    {
        var path = Path.Combine(_dataDirectory, LoadResult.FileName);

        var deadline = Stopwatch.GetTimestamp() + (long)(timeout.TotalSeconds * Stopwatch.Frequency);

        while (!File.Exists(path))
        {
            ThrowIfExitedOrLate(deadline, "result.json");

            await Task.Delay(100);
        }

        return LoadResult.Read(path);
    }

    public async Task WaitForAcceptedAsync(int minimum, TimeSpan timeout)
    {
        var deadline = Stopwatch.GetTimestamp() + (long)(timeout.TotalSeconds * Stopwatch.Frequency);

        while (ReadAcceptedIds().Count < minimum)
        {
            ThrowIfExitedOrLate(deadline, $"{minimum} accepted ids");

            await Task.Delay(20);
        }
    }

    // A killed process can leave a cut last line, so only whole 32-character ids count
    public IReadOnlyList<string> ReadAcceptedIds()
    {
        var path = Path.Combine(_dataDirectory, LoadRunner.AcceptedIdsFileName);

        if (!File.Exists(path))
        {
            return [];
        }

        using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);

        using var reader = new StreamReader(file);

        return reader.ReadToEnd().Split('\n').Where(id => id.Length == 32).ToList();
    }

    public void Kill()
    {
        _process.Kill(entireProcessTree: true);

        _process.WaitForExit();
    }

    public TimeSpan CpuTime()
    {
        _process.Refresh();

        return _process.TotalProcessorTime;
    }

    public long PeakMemoryBytes()
    {
        _process.Refresh();

        _peakMemory = Math.Max(_peakMemory, _process.PeakWorkingSet64);

        return _peakMemory;
    }

    public async ValueTask DisposeAsync()
    {
        if (!_process.HasExited)
        {
            _process.Kill(entireProcessTree: true);

            await _process.WaitForExitAsync();
        }

        _process.Dispose();

        // Filebeat may still hold a deleted log open, which is harmless
        if (Directory.Exists(_runDirectory))
        {
            Directory.Delete(_runDirectory, recursive: true);
        }
    }

    private void Keep(string? line)
    {
        if (line is null)
        {
            return;
        }

        _output.Enqueue(line);

        // Only the last lines matter when the app fails
        while (_output.Count > 50)
        {
            _output.TryDequeue(out _);
        }
    }

    private void ThrowIfExitedOrLate(long deadline, string waitingFor)
    {
        if (_process.HasExited)
        {
            throw new InvalidOperationException($"DemoApi exited with code {_process.ExitCode} before {waitingFor}:\n{string.Join('\n', _output)}");
        }

        if (Stopwatch.GetTimestamp() > deadline)
        {
            throw new TimeoutException($"DemoApi did not produce {waitingFor} in time:\n{string.Join('\n', _output)}");
        }
    }
}
