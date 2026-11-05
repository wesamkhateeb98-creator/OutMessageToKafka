namespace DemoApi;

/// <summary>Each process runs exactly one solution, so background workers never disturb another benchmark.</summary>
public static class Solutions
{
    public static void AddSolution(this IServiceCollection services, string name)
    {
        switch (name)
        {
            case "await-produce":
                services.AddSingleton<IMessageAcceptor, AwaitProduceAcceptor>();
                break;

            case "outbox":
                services.AddAcceptorWithRelay<PostgresOutbox>();
                break;

            case "local-wal":
            case "gateway-wal":
                services.AddAcceptorWithRelay<GroupCommitWal>();
                break;

            case "blob":
                services.AddAcceptorWithRelay<BlobBuffer>();
                break;

            case "immediate-fsync":
                services.AddAcceptorWithRelay<ImmediateWal>();
                break;

            case "zlogger":
                services.AddSingleton<IMessageAcceptor, ZLoggerAcceptor>();
                break;

            case "sqlite":
                services.AddAcceptorWithRelay<SqliteOutbox>();
                break;

            default:
                throw new ArgumentException($"Unknown solution: {name}", nameof(name));
        }
    }

    private static void AddAcceptorWithRelay<TSolution>(this IServiceCollection services)
        where TSolution : class, IMessageAcceptor, IRelaySource, IHostedService
    {
        services.AddSingleton<TSolution>();

        services.AddSingleton<IMessageAcceptor>(provider => provider.GetRequiredService<TSolution>());

        services.AddSingleton<IRelaySource>(provider => provider.GetRequiredService<TSolution>());

        services.AddHostedService(provider => provider.GetRequiredService<TSolution>());

        services.AddHostedService<RelayWorker>();
    }
}
