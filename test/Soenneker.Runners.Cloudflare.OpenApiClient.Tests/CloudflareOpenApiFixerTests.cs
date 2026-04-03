using Soenneker.Facts.Local;
using Soenneker.Tests.FixturedUnit;
using System.Threading.Tasks;
using Soenneker.Extensions.ValueTask;
using Soenneker.Kiota.Util.Abstract;
using Xunit;
using Soenneker.OpenApi.Fixer.Abstract;

namespace Soenneker.Runners.Cloudflare.OpenApiClient.Tests;

[Collection("Collection")]
public class CloudflareOpenApiFixerTests : FixturedUnitTest
{
    private readonly IOpenApiFixer _fixer;
    private readonly IKiotaUtil _util;

    public CloudflareOpenApiFixerTests(Fixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
        _fixer = Resolve<IOpenApiFixer>(true);
        _util = Resolve<IKiotaUtil>(true);
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
        await _util.Generate("c:\\cloudflare\\fixed.json", "CloudflareOpenApiClient", Constants.Library, @"c:\cloudflare\dir", CancellationToken).NoSync();
    }

    [LocalFact]
    public async ValueTask FixAndGenerate()
    {
        await Fix();
        await Generate();
    }
}
