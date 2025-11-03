using Soenneker.Facts.Local;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.Tests.FixturedUnit;
using System.Threading.Tasks;
using Soenneker.Extensions.ValueTask;
using Xunit;

namespace Soenneker.Runners.Cloudflare.OpenApiClient.Tests;

[Collection("Collection")]
public class CloudflareOpenApiFixerTests : FixturedUnitTest
{
    private IOpenApiFixer _fixer;

    public CloudflareOpenApiFixerTests(Fixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
        _fixer = Resolve<IOpenApiFixer>(true);
    }

    [Fact]
    public void Default()
    {

    }

    [LocalFact]
    public async ValueTask Fix()
    { 
        await _fixer.Fix("c:\\cloudflare\\unformatted.json", "c:\\cloudflare\\fixed1.json");
    }

    [LocalFact]
    public async ValueTask Generate()
    {
        await _fixer.GenerateKiota("c:\\cloudflare\\fixed.json", "CloudflareOpenApiClient", Constants.Library, @"c:\cloudflare\dir", CancellationToken).NoSync();

    }
}
