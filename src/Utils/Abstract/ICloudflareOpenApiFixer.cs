using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Runners.Cloudflare.OpenApiClient.Utils.Abstract;

public interface ICloudflareOpenApiFixer
{
    ValueTask Fix(string sourceFilePath, string targetFilePath, CancellationToken cancellationToken = default);
}