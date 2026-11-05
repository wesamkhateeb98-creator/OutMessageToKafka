using DemoApi;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.ClearProviders();

var options = DemoOptions.From(builder.Configuration);

Directory.CreateDirectory(options.DataDirectory);

builder.Services.AddSingleton(options);

builder.Services.AddSingleton<KafkaSender>();

builder.Services.AddSolution(builder.Configuration.GetValue<string>("Demo:Solution")
    ?? throw new InvalidOperationException("Demo:Solution is required."));

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
