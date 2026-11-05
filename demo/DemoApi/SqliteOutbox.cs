using Microsoft.Data.Sqlite;

namespace DemoApi;

/// <summary>docs/sqlite-local-outbox.md: WAL mode with synchronous=FULL, a single writer, and group commit.</summary>
public sealed class SqliteOutbox : BackgroundService, IMessageAcceptor, IRelaySource
{
    private readonly SqliteConnection _writer;

    private readonly SqliteConnection _relay;

    private readonly SqliteCommand _insert;

    private readonly GroupCommitQueue _queue;

    private long _pendingUpTo;

    public SqliteOutbox(DemoOptions options)
    {
        var path = Path.Combine(options.DataDirectory, "outbox.db");

        _writer = Open(path);

        _relay = Open(path);

        using var create = _writer.CreateCommand();

        create.CommandText = "CREATE TABLE IF NOT EXISTS outbox(seq INTEGER PRIMARY KEY AUTOINCREMENT, payload BLOB NOT NULL)";

        create.ExecuteNonQuery();

        _insert = _writer.CreateCommand();

        _insert.CommandText = "INSERT INTO outbox(payload) VALUES($payload)";

        _insert.Parameters.Add("$payload", SqliteType.Blob);

        _insert.Prepare();

        _queue = new GroupCommitQueue(CommitAsync, TimeSpan.Zero);
    }

    private static SqliteConnection Open(string path)
    {
        var connection = new SqliteConnection($"Data Source={path};Pooling=False");

        connection.Open();

        using var pragma = connection.CreateCommand();

        pragma.CommandText = "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;";

        pragma.ExecuteNonQuery();

        return connection;
    }

    public Task AcceptAsync(DemoMessage message, CancellationToken ct) => _queue.EnqueueAsync(message, ct);

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.Run(() => _queue.RunAsync(stoppingToken), stoppingToken);

    private Task CommitAsync(IReadOnlyList<DemoMessage> batch)
    {
        using var transaction = _writer.BeginTransaction();

        _insert.Transaction = transaction;

        foreach (var message in batch)
        {
            _insert.Parameters["$payload"].Value = message.ToJson();

            _insert.ExecuteNonQuery();
        }

        transaction.Commit();

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<DemoMessage>> ReadBatchAsync(CancellationToken ct)
    {
        var batch = new List<DemoMessage>(1000);

        using var select = _relay.CreateCommand();

        select.CommandText = "SELECT seq, payload FROM outbox ORDER BY seq LIMIT 1000";

        using var reader = select.ExecuteReader();

        while (reader.Read())
        {
            _pendingUpTo = reader.GetInt64(0);

            batch.Add(DemoMessage.FromJson(reader.GetFieldValue<byte[]>(1)));
        }

        return Task.FromResult<IReadOnlyList<DemoMessage>>(batch);
    }

    public Task AcknowledgeAsync(CancellationToken ct)
    {
        using var delete = _relay.CreateCommand();

        delete.CommandText = "DELETE FROM outbox WHERE seq <= $upTo";

        delete.Parameters.AddWithValue("$upTo", _pendingUpTo);

        delete.ExecuteNonQuery();

        return Task.CompletedTask;
    }

    public override void Dispose()
    {
        _insert.Dispose();

        _writer.Dispose();

        _relay.Dispose();

        base.Dispose();
    }
}
