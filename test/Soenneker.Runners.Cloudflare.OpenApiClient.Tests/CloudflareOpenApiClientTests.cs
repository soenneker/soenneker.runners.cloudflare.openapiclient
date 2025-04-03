using Soenneker.Runners.Cloudflare.OpenApiClient.Abstract;
using Soenneker.Tests.FixturedUnit;
using Xunit;

namespace Soenneker.Runners.Cloudflare.OpenApiClient.Tests;

[Collection("Collection")]
public class CloudflareOpenApiClientTests : FixturedUnitTest
{
    private readonly ICloudflareOpenApiClient _runner;

    public CloudflareOpenApiClientTests(Fixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
        _runner = Resolve<ICloudflareOpenApiClient>(true);
    }

    [Fact]
    public void Default()
    {

    }
}
