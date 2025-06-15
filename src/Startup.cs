using Microsoft.Extensions.DependencyInjection;
using Soenneker.Managers.Runners.Registrars;
using Soenneker.OpenApi.Fixer.Registrars;
using Soenneker.Runners.Cloudflare.OpenApiClient.Utils;
using Soenneker.Runners.Cloudflare.OpenApiClient.Utils.Abstract;
using Soenneker.Utils.File.Download.Registrars;
using Soenneker.Utils.Usings.Registrars;

namespace Soenneker.Runners.Cloudflare.OpenApiClient;

/// <summary>
/// Console type startup
/// </summary>
public static class Startup
{
    // This method gets called by the runtime. Use this method to add services to the container.
    public static void ConfigureServices(IServiceCollection services)
    {
        services.SetupIoC();
    }

    public static IServiceCollection SetupIoC(this IServiceCollection services)
    {
        services.AddHostedService<ConsoleHostedService>()
                .AddScoped<IFileOperationsUtil, FileOperationsUtil>()
                .AddRunnersManagerAsScoped()
                .AddFileDownloadUtilAsScoped()
                .AddUsingsUtilAsScoped()
                .AddOpenApiFixerAsScoped();

        return services;
    }
}