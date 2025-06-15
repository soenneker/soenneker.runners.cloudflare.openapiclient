using Soenneker.Facts.Local;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.Tests.FixturedUnit;
using System.Threading.Tasks;
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
        var cloudflareOpenApiFixer = Resolve<IOpenApiFixer>(true);

        await cloudflareOpenApiFixer.Fix("c:\\cloudflare\\unformatted.json", "c:\\cloudflare\\fixed.json");
    }
}
