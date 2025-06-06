using Soenneker.Facts.Local;
using Soenneker.Tests.FixturedUnit;
using System.Threading.Tasks;
using Soenneker.Runners.Cloudflare.OpenApiClient.Utils.Abstract;
using Xunit;

namespace Soenneker.Runners.Cloudflare.OpenApiClient.Tests;

[Collection("Collection")]
public class CloudflareOpenApiFixerTests : FixturedUnitTest
{
    public CloudflareOpenApiFixerTests(Fixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
    }

    [Fact]
    public void Default()
    {

    }

    [LocalFact]
    public async ValueTask Fix()
    { 
        var cloudflareOpenApiFixer = Resolve<ICloudflareOpenApiFixer>(true);

        await cloudflareOpenApiFixer.Fix("c:\\cloudflare\\input.json", "c:\\cloudflare\\fixed.json");
    }
}
