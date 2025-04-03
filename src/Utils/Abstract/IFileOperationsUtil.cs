using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Runners.Cloudflare.OpenApiClient.Utils.Abstract;

public interface IFileOperationsUtil
{
    ValueTask Process(CancellationToken cancellationToken = default);
}
