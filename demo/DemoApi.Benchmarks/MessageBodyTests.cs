using System.Text;
using System.Text.Json;
using Xunit;

namespace DemoApi.Benchmarks;

public sealed class MessageBodyTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(9_999)]
    [InlineData(int.MaxValue)]
    public void BuildMessage_AnyIndex_NameAndEmailAreExactlyOneKilobyteOfJson(int index)
    {
        // Act
        var message = LoadRunner.BuildMessage(index);

        // Assert
        var body = JsonSerializer.Serialize(new { name = message.Name, email = message.Email });

        Assert.Equal(1024, Encoding.UTF8.GetByteCount(body));

        Assert.Equal($"user-{index}@example.com", message.Email);

        Assert.Equal(32, message.Id.Length);
    }
}
