using Xunit;

namespace Grafeo.Tests;

/// <summary>Database lifecycle, info, and admin tests.</summary>
public sealed class DatabaseTests : IDisposable
{
    private readonly GrafeoDB _db = GrafeoDB.Memory();

    public void Dispose() => _db.Dispose();

    [Fact]
    public void OpensInMemoryDatabase()
    {
        Assert.Equal(0, _db.NodeCount);
        Assert.Equal(0, _db.EdgeCount);
    }

    [Fact]
    public void DoubleDisposeIsNoOp()
    {
        using var db = GrafeoDB.Memory();
        db.Dispose();
        db.Dispose(); // should not throw
    }

    [Fact]
    public void ThrowsOnUseAfterDispose()
    {
        var db = GrafeoDB.Memory();
        db.Dispose();
        Assert.Throws<ObjectDisposedException>(() => db.Execute("RETURN 1"));
    }

    [Fact]
    public void ReturnsVersion()
    {
        var version = GrafeoDB.Version;
        Assert.NotNull(version);
        Assert.NotEqual("unknown", version);
        Assert.Contains('.', version); // semver: X.Y.Z
    }

    [Fact]
    public void ReturnsInfo()
    {
        var info = _db.Info();
        Assert.NotNull(info);
        Assert.True(info.Count > 0);
    }

}
