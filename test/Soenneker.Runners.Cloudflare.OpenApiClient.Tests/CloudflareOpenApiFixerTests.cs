using Soenneker.Facts.Local;
using Soenneker.Tests.FixturedUnit;
using System.Threading.Tasks;
using Soenneker.Extensions.ValueTask;
using Xunit;
using Soenneker.OpenApi.Fixer.Abstract;

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
        await _fixer.Fix("c:\\cloudflare\\openapi.json", "c:\\cloudflare\\fixed.json");
    }

    [LocalFact]
    public async ValueTask Generate()
    {
        await _fixer.GenerateKiota("c:\\cloudflare\\fixed.json", "CloudflareOpenApiClient", Constants.Library, @"c:\cloudflare\dir", CancellationToken).NoSync();
    }

    [LocalFact]
    public async ValueTask FixAndGenerate()
    {
        await Fix();
        await Generate();
    }
}
