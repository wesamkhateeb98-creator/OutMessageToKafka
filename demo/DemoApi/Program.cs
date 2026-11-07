using DemoApi;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.ClearProviders();

var solution = builder.Configuration["solution"]
    ?? throw new InvalidOperationException("--solution=<s1|s2|s3|s4|s5|s6> is required.");

// Only s2, s4, and s6 have their own file. The command line is added again so it still wins over the files
builder.Configuration.AddJsonFile($"appsettings.{solution}.json", optional: true);

builder.Configuration.AddCommandLine(args);

var options = DemoOptions.From(builder.Configuration);

Directory.CreateDirectory(options.DataDirectory);

builder.Services.AddSingleton(options);

builder.Services.AddSingleton<KafkaSender>();

// Each process runs exactly one solution, so background workers never disturb another benchmark
switch (solution)
{
    case "s1":
        builder.Services.AddSingleton<IMessageAcceptor, AwaitProduceAcceptor>();

        break;

    case "s2":
        builder.Services.AddSingleton(builder.Configuration.GetRequiredSettings<BlobSettings>("Blob"));

        AddAcceptorWithRelay<BlobBuffer>(builder.Services);

        break;

    case "s3":
        AddAcceptorWithRelay<GroupCommitWal>(builder.Services);

        break;

    case "s4":
        builder.Services.AddSingleton(builder.Configuration.GetRequiredSettings<PostgresSettings>("Postgres"));

        AddAcceptorWithRelay<PostgresOutbox>(builder.Services);

        break;

    case "s5":
        AddAcceptorWithRelay<SqliteOutbox>(builder.Services);

        break;

    case "s6":
        builder.Services.AddSingleton(builder.Configuration.GetRequiredSettings<DurableLogSettings>("ZLogger"));

        builder.Services.AddSingleton<IMessageAcceptor, ZLoggerAcceptor>();

        break;

    default:
        throw new ArgumentException($"Unknown solution: {solution}. Use s1, s2, s3, s4, s5, or s6.");
}

var app = builder.Build();

app.MapGet("/health", () => Results.Ok());

app.MapPost("/messages", async (IncomingMessage request, HttpContext http, IMessageAcceptor acceptor, CancellationToken ct) =>
{
    var headerId = http.Request.Headers["X-Message-Id"].ToString();

    var id = string.IsNullOrEmpty(headerId) ? Guid.CreateVersion7().ToString("N") : headerId;

    try
    {
        await acceptor.AcceptAsync(new DemoMessage(id, request.Name, request.Email), ct);
    }
    catch (Exception) when (!ct.IsCancellationRequested)
    {
        return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
    }

    http.Response.Headers["X-Message-Id"] = id;

    return Results.Accepted();
});

app.Run();

static void AddAcceptorWithRelay<TSolution>(IServiceCollection services)
    where TSolution : class, IMessageAcceptor, IRelaySource, IHostedService
{
    services.AddSingleton<TSolution>();

    services.AddSingleton<IMessageAcceptor>(provider => provider.GetRequiredService<TSolution>());

    services.AddSingleton<IRelaySource>(provider => provider.GetRequiredService<TSolution>());

    services.AddHostedService(provider => provider.GetRequiredService<TSolution>());

    services.AddHostedService<RelayWorker>();
}
