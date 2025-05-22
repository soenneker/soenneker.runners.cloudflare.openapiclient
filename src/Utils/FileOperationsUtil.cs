using Microsoft.Extensions.Logging;
using Soenneker.Extensions.String;
using Soenneker.Extensions.ValueTask;
using Soenneker.Git.Util.Abstract;
using Soenneker.Runners.Cloudflare.OpenApiClient.Utils.Abstract;
using Soenneker.Utils.Dotnet.Abstract;
using Soenneker.Utils.Environment;
using Soenneker.Utils.File.Download.Abstract;
using Soenneker.Utils.FileSync.Abstract;
using Soenneker.Utils.Process.Abstract;
using Soenneker.Utils.Usings;
using Soenneker.Utils.Usings.Abstract;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Runners.Cloudflare.OpenApiClient.Utils;

///<inheritdoc cref="IFileOperationsUtil"/>
public class FileOperationsUtil : IFileOperationsUtil
{
    private readonly ILogger<FileOperationsUtil> _logger;
    private readonly IGitUtil _gitUtil;
    private readonly IDotnetUtil _dotnetUtil;
    private readonly IProcessUtil _processUtil;
    private readonly ICloudflareOpenApiFixer _cloudflareOpenApiFixer;
    private readonly IFileDownloadUtil _fileDownloadUtil;
    private readonly IFileUtilSync _fileUtilSync;
    private readonly IUsingsUtil _usingsUtil;

    public FileOperationsUtil(ILogger<FileOperationsUtil> logger, IGitUtil gitUtil, IDotnetUtil dotnetUtil, IProcessUtil processUtil,
        ICloudflareOpenApiFixer cloudflareOpenApiFixer, IFileDownloadUtil fileDownloadUtil, IFileUtilSync fileUtilSync, IUsingsUtil usingsUtil)
    {
        _logger = logger;
        _gitUtil = gitUtil;
        _dotnetUtil = dotnetUtil;
        _processUtil = processUtil;
        _cloudflareOpenApiFixer = cloudflareOpenApiFixer;
        _fileDownloadUtil = fileDownloadUtil;
        _fileUtilSync = fileUtilSync;
        _usingsUtil = usingsUtil;
    }

    public async ValueTask Process(CancellationToken cancellationToken = default)
    {
        string gitDirectory = _gitUtil.CloneToTempDirectory($"https://github.com/soenneker/{Constants.Library.ToLowerInvariantFast()}");

        string targetFilePath = Path.Combine(gitDirectory, "openapi.json");

        _fileUtilSync.DeleteIfExists(targetFilePath);

        string? filePath = await _fileDownloadUtil.Download("https://raw.githubusercontent.com/cloudflare/api-schemas/refs/heads/main/openapi.json",
            targetFilePath, fileExtension: ".json", cancellationToken: cancellationToken);

        await _processUtil.Start("dotnet", null, "tool update --global Microsoft.OpenApi.Kiota", waitForExit: true, cancellationToken: cancellationToken);

        string fixedFilePath = Path.Combine(gitDirectory, "fixed.json");

        await _cloudflareOpenApiFixer.Fix(filePath, fixedFilePath, cancellationToken).NoSync();

        string srcDirectory = Path.Combine(gitDirectory, "src");

        DeleteAllExceptCsproj(srcDirectory);

        await _processUtil.Start("kiota", gitDirectory, $"kiota generate -l CSharp -d \"{fixedFilePath}\" -o src -c CloudflareOpenApiClient -n {Constants.Library}",
            waitForExit: true, cancellationToken: cancellationToken).NoSync();

        // **NEW** post‐process the generated code
        PostProcessKiotaClient(srcDirectory);

        string projFilePath = Path.Combine(gitDirectory, "src", $"{Constants.Library}.csproj");

        await _dotnetUtil.Restore(projFilePath, cancellationToken: cancellationToken);

        await _usingsUtil.AddMissing(Path.Combine(srcDirectory, Constants.Library + ".csproj"));

        //   await BuildAndPush(gitDirectory, cancellationToken).NoSync();
    }

    public void DeleteAllExceptCsproj(string directoryPath)
    {
        if (!Directory.Exists(directoryPath))
        {
            _logger.LogWarning("Directory does not exist: {DirectoryPath}", directoryPath);
            return;
        }

        try
        {
            // Delete all files except .csproj
            foreach (string file in Directory.GetFiles(directoryPath, "*", SearchOption.AllDirectories))
            {
                if (!file.EndsWith(".csproj", StringComparison.OrdinalIgnoreCase))
                {
                    try
                    {
                        File.Delete(file);
                        _logger.LogInformation("Deleted file: {FilePath}", file);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to delete file: {FilePath}", file);
                    }
                }
            }

            // Delete all empty subdirectories
            foreach (string dir in Directory.GetDirectories(directoryPath, "*", SearchOption.AllDirectories)
                                            .OrderByDescending(d => d.Length)) // Sort by depth to delete from deepest first
            {
                try
                {
                    if (Directory.Exists(dir) && !Directory.EnumerateFileSystemEntries(dir).Any())
                    {
                        Directory.Delete(dir, recursive: false);
                        _logger.LogInformation("Deleted empty directory: {DirectoryPath}", dir);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to delete directory: {DirectoryPath}", dir);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "An error occurred while cleaning the directory: {DirectoryPath}", directoryPath);
        }
    }

    // call this right after kiota generate
    private void PostProcessKiotaClient(string srcDirectory)
    {
        var csFiles = Directory.GetFiles(srcDirectory, "*.cs", SearchOption.AllDirectories);

        // Regex to find properties whose name starts with a digit
        var propDeclRx = new Regex(@"public\s+([\w<>,\?\[\]]+)\s+([0-9]\w*)");
        // Regex to fix GetFieldDeserializers entries
        var deserRx = new Regex(@"\{\s*""(?<json>_[A-Za-z0-9_]+)""\s*,\s*n\s*=>\s*\{\s*(?<name>[0-9]\w*)");
        // Regex to fix all Write*Value calls in Serialize
        var writeRx = new Regex(@"\bWrite(?:StringValue|BoolValue|NumberValue|CollectionOfPrimitiveValues<[^>]+>|CollectionOfObjectValues<[^>]+>|ObjectValue<[^>]+>)" +
                                @"\(\s*""(?<json>_[A-Za-z0-9_]+)""\s*,\s*(?<name>[0-9]\w*)\s*\)");

        foreach (var file in csFiles)
        {
            var text = File.ReadAllText(file);
            var original = text;

            // 1) Prefix any public property starting with a digit
            text = propDeclRx.Replace(text, m =>
                $"public {m.Groups[1].Value} N{m.Groups[2].Value}");

            // 2) In deserializer, assign into the new name but keep JSON key
            text = deserRx.Replace(text, m =>
                $"{{ \"{m.Groups["json"].Value}\", n => {{ N{m.Groups["name"].Value}");

            // 3) In Serialize, use new name but still write original JSON key
            text = writeRx.Replace(text, m =>
                $"{m.Value.Substring(0, m.Value.IndexOf('(') + 1)}\"{m.Groups["json"].Value}\", N{m.Groups["name"].Value})");

            if (text != original)
                File.WriteAllText(file, text);
        }
    }


    private async ValueTask BuildAndPush(string gitDirectory, CancellationToken cancellationToken)
    {
        string projFilePath = Path.Combine(gitDirectory, "src", $"{Constants.Library}.csproj");

        await _dotnetUtil.Restore(projFilePath, cancellationToken: cancellationToken);

        bool successful = await _dotnetUtil.Build(projFilePath, true, "Release", false, cancellationToken: cancellationToken);

        if (!successful)
        {
            _logger.LogError("Build was not successful, exiting...");
            return;
        }

        string gitHubToken = EnvironmentUtil.GetVariableStrict("GH_TOKEN");

        await _gitUtil.CommitAndPush(gitDirectory, "soenneker", "Jake Soenneker", "jake@soenneker.com", gitHubToken, "Automated update");
    }
}