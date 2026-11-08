using Xunit;

namespace DemoApi.Benchmarks;

public sealed class MedianTests
{
    [Theory]
    [InlineData(new[] { 6.1, 151.0, 7.1 }, "7.1")]
    [InlineData(new[] { 6.1, 7.1, 9.0, 151.0 }, "8.1")]
    [InlineData(new[] { 42.0 }, "42.0")]
    [InlineData(new double[0], "")]
    public void Median_Values_ReturnsMiddleNotMean(double[] values, string expected)
    {
        // Act
        var median = BenchmarkResult.Median(values, "F1");

        // Assert
        Assert.Equal(expected, median);
    }
}
