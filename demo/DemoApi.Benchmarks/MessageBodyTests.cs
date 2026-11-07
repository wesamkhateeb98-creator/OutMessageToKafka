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
    public void BuildBody_AnyIndex_IsExactlyOneKilobyteOfValidJson(int index)
    {
        // Act
        var body = LoadGenerator.BuildBody(index);

        // Assert
        Assert.Equal(1024, Encoding.UTF8.GetByteCount(body));

        using var json = JsonDocument.Parse(body);

        Assert.Equal($"user-{index}@example.com", json.RootElement.GetProperty("email").GetString());
    }
}
