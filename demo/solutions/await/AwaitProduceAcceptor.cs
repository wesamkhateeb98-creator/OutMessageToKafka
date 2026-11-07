namespace DemoApi;

/// <summary>docs/await-produce-async.md: reply only after Kafka acknowledged with acks=all.</summary>
public sealed class AwaitProduceAcceptor(KafkaSender sender) : IMessageAcceptor
{
    public Task AcceptAsync(DemoMessage message, CancellationToken ct) => sender.SendAsync(message, ct);
}
