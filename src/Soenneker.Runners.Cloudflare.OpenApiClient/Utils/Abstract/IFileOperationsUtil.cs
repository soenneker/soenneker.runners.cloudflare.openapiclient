using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Runners.Cloudflare.OpenApiClient.Utils.Abstract;

/// <summary>
/// Defines the file operations util contract.
/// </summary>
public interface IFileOperationsUtil
{
    /// <summary>
    /// Executes the process operation.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    ValueTask Process(CancellationToken cancellationToken = default);
}
