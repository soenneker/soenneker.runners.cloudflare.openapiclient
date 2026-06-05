using Microsoft.Extensions.DependencyInjection;
using Soenneker.Kiota.Util.Registrars;
using Soenneker.Managers.Runners.Registrars;
using Soenneker.OpenApi.Fixer.Registrars;
using Soenneker.Runners.Cloudflare.OpenApiClient.Utils;
using Soenneker.Runners.Cloudflare.OpenApiClient.Utils.Abstract;
using Soenneker.Utils.File.Download.Registrars;

namespace Soenneker.Runners.Cloudflare.OpenApiClient;

/// <summary>
/// Console type startup
/// </summary>
public static class Startup
{
    // This method gets called by the runtime. Use this method to add services to the container.
    /// <summary>
    /// Configures services.
    /// </summary>
    /// <param name="services">The service collection.</param>
    public static void ConfigureServices(IServiceCollection services)
    {
        services.SetupIoC();
    }

    /// <summary>
    /// Sets up io c.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The result of the operation.</returns>
    public static IServiceCollection SetupIoC(this IServiceCollection services)
    {
        services.AddHostedService<ConsoleHostedService>()
                .AddSingleton<IFileOperationsUtil, FileOperationsUtil>()
                .AddRunnersManagerAsSingleton()
                .AddFileDownloadUtilAsSingleton()
                .AddOpenApiFixerAsSingleton()
                .AddKiotaUtilAsSingleton();

        return services;
    }
}