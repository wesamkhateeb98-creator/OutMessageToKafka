using Npgsql;

namespace DemoApi;

/// <summary>docs/outbox-pattern.md: the business row and the outbox row commit in one transaction.</summary>
public sealed class PostgresOutbox(DemoOptions options) : BackgroundService, IMessageAcceptor, IRelaySource
{
    private readonly NpgsqlDataSource _dataSource = NpgsqlDataSource.Create(options.PostgresConnection);

    private readonly List<long> _pending = [];

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await using var command = _dataSource.CreateCommand("""
            CREATE TABLE IF NOT EXISTS users(id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox(seq BIGSERIAL PRIMARY KEY, id TEXT NOT NULL, payload BYTEA NOT NULL);
            TRUNCATE users, outbox;
            """);

        await command.ExecuteNonQueryAsync(cancellationToken);

        await base.StartAsync(cancellationToken);
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.CompletedTask;

    public async Task AcceptAsync(DemoMessage message, CancellationToken ct)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(ct);

        await using var transaction = await connection.BeginTransactionAsync(ct);

        await using var batch = new NpgsqlBatch(connection, transaction);

        var insertUser = new NpgsqlBatchCommand("INSERT INTO users(id, name, email) VALUES($1, $2, $3)");

        insertUser.Parameters.AddWithValue(message.Id);

        insertUser.Parameters.AddWithValue(message.Name);

        insertUser.Parameters.AddWithValue(message.Email);

        batch.BatchCommands.Add(insertUser);

        var insertOutbox = new NpgsqlBatchCommand("INSERT INTO outbox(id, payload) VALUES($1, $2)");

        insertOutbox.Parameters.AddWithValue(message.Id);

        insertOutbox.Parameters.AddWithValue(message.ToJson());

        batch.BatchCommands.Add(insertOutbox);

        await batch.ExecuteNonQueryAsync(ct);

        await transaction.CommitAsync(ct);
    }

    public async Task<IReadOnlyList<DemoMessage>> ReadBatchAsync(CancellationToken ct)
    {
        _pending.Clear();

        var batch = new List<DemoMessage>(1000);

        await using var command = _dataSource.CreateCommand("SELECT seq, payload FROM outbox ORDER BY seq LIMIT 1000");

        await using var reader = await command.ExecuteReaderAsync(ct);

        while (await reader.ReadAsync(ct))
        {
            _pending.Add(reader.GetInt64(0));

            batch.Add(DemoMessage.FromJson(reader.GetFieldValue<byte[]>(1)));
        }

        return batch;
    }

    public async Task AcknowledgeAsync(CancellationToken ct)
    {
        await using var command = _dataSource.CreateCommand("DELETE FROM outbox WHERE seq = ANY($1)");

        command.Parameters.AddWithValue(_pending.ToArray());

        await command.ExecuteNonQueryAsync(ct);
    }
}
