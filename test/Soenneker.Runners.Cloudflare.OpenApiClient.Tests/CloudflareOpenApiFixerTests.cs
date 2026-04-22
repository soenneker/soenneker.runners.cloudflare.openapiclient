using Soenneker.Tests.Attributes.Local;
using Soenneker.Tests.HostedUnit;
using System.Threading.Tasks;
using Soenneker.Extensions.ValueTask;
using Soenneker.Kiota.Util.Abstract;
using Soenneker.OpenApi.Fixer.Abstract;

namespace Soenneker.Runners.Cloudflare.OpenApiClient.Tests;

[ClassDataSource<Host>(Shared = SharedType.PerTestSession)]
public class CloudflareOpenApiFixerTests : HostedUnitTest
{
    private readonly IOpenApiFixer _fixer;
    private readonly IKiotaUtil _util;

    public CloudflareOpenApiFixerTests(Host host) : base(host)
    {
        _fixer = Resolve<IOpenApiFixer>(true);
        _util = Resolve<IKiotaUtil>(true);
    }

    [Test]
    public void Default()
    {

    }

    [LocalOnly]
    public async ValueTask Fix()
    { 
        await _fixer.Fix("c:\\cloudflare\\openapi.json", "c:\\cloudflare\\fixed.json");
    }

    [LocalOnly]
    public async ValueTask Generate()
    {
        await _util.Generate("c:\\cloudflare\\fixed.json", "CloudflareOpenApiClient", Constants.Library, @"c:\cloudflare\dir", CancellationToken).NoSync();
    }

    [LocalOnly]
    public async ValueTask FixAndGenerate()
    {
        await Fix();
        await Generate();
    }
}
