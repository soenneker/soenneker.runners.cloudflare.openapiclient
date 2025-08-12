using Microsoft.Extensions.Logging;
using Soenneker.Extensions.String;
using Soenneker.Extensions.ValueTask;
using Soenneker.Git.Util.Abstract;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.Runners.Cloudflare.OpenApiClient.Utils.Abstract;
using Soenneker.Utils.Dotnet.Abstract;
using Soenneker.Utils.Environment;
using Soenneker.Utils.File.Download.Abstract;
using Soenneker.Utils.Process.Abstract;
using Soenneker.Utils.Usings.Abstract;
using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.Utils.File.Abstract;

namespace Soenneker.Runners.Cloudflare.OpenApiClient.Utils;

///<inheritdoc cref="IFileOperationsUtil"/>
public sealed class FileOperationsUtil : IFileOperationsUtil
{
    private readonly ILogger<FileOperationsUtil> _logger;
    private readonly IGitUtil _gitUtil;
    private readonly IDotnetUtil _dotnetUtil;
    private readonly IProcessUtil _processUtil;
    private readonly IOpenApiFixer _openApiFixer;
    private readonly IFileDownloadUtil _fileDownloadUtil;
    private readonly IFileUtil _fileUtil;
    private readonly IUsingsUtil _usingsUtil;

    public FileOperationsUtil(ILogger<FileOperationsUtil> logger, IGitUtil gitUtil, IDotnetUtil dotnetUtil, IProcessUtil processUtil,
        IOpenApiFixer openApiFixer, IFileDownloadUtil fileDownloadUtil, IFileUtil fileUtil, IUsingsUtil usingsUtil)
    {
        _logger = logger;
        _gitUtil = gitUtil;
        _dotnetUtil = dotnetUtil;
        _processUtil = processUtil;
        _openApiFixer = openApiFixer;
        _fileDownloadUtil = fileDownloadUtil;
        _fileUtil = fileUtil;
        _usingsUtil = usingsUtil;
    }

    public async ValueTask Process(CancellationToken cancellationToken = default)
    {
        string gitDirectory = await _gitUtil.CloneToTempDirectory($"https://github.com/soenneker/{Constants.Library.ToLowerInvariantFast()}",
            cancellationToken: cancellationToken);

        string targetFilePath = Path.Combine(gitDirectory, "openapi.json");

        await _fileUtil.DeleteIfExists(targetFilePath, cancellationToken: cancellationToken);

        string? filePath = await _fileDownloadUtil.Download("https://raw.githubusercontent.com/cloudflare/api-schemas/refs/heads/main/openapi.json",
            targetFilePath, fileExtension: ".json", cancellationToken: cancellationToken);

        await _processUtil.Start("dotnet", null, "tool update --global Microsoft.OpenApi.Kiota", waitForExit: true, cancellationToken: cancellationToken);

        string fixedFilePath = Path.Combine(gitDirectory, "fixed.json");

        await _openApiFixer.Fix(filePath, fixedFilePath, cancellationToken).NoSync();

        string srcDirectory = Path.Combine(gitDirectory, "src");

        DeleteAllExceptCsproj(srcDirectory);

        await _processUtil.Start("kiota", gitDirectory,
                              $"kiota generate -l CSharp -d \"{fixedFilePath}\" -o src -c CloudflareOpenApiClient -n {Constants.Library}", waitForExit: true,
                              cancellationToken: cancellationToken)
                          .NoSync();

        await PostProcessKiotaClient(srcDirectory, cancellationToken);
        await FixEnumIdToString(srcDirectory, cancellationToken);

        await FixPrimitiveCollectionNullability(srcDirectory, cancellationToken);

        await FixExpiryDefault(srcDirectory, cancellationToken);
        await FixStringListDefaults(srcDirectory, cancellationToken);

        string projFilePath = Path.Combine(gitDirectory, "src", $"{Constants.Library}.csproj");

        await _dotnetUtil.Restore(projFilePath, cancellationToken: cancellationToken);

        await _usingsUtil.AddMissing(projFilePath, true, 6, cancellationToken);

        await BuildAndPush(gitDirectory, cancellationToken).NoSync();
    }

    private async ValueTask FixStringListDefaults(string srcDirectory, CancellationToken cancellationToken = default)
    {
        // Matches: PropertyName = "someValue";
        // where PropertyName is Headers or ExpectedCodes (add more as needed)
        var rx = new Regex(@"\b(?<prop>Headers|ExpectedCodes)\s*=\s*""(?<val>[^""]+)""\s*;", RegexOptions.Multiline);

        foreach (string file in Directory.GetFiles(srcDirectory, "*.cs", SearchOption.AllDirectories))
        {
            string text = await File.ReadAllTextAsync(file, cancellationToken);
            string newText = rx.Replace(text, m =>
            {
                string prop = m.Groups["prop"].Value;
                string val = m.Groups["val"].Value;
                return $"{prop} = new List<string> {{ \"{val}\" }};";
            });

            if (newText != text)
                await File.WriteAllTextAsync(file, newText, cancellationToken);
        }
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
    private static async ValueTask PostProcessKiotaClient(string srcDirectory, CancellationToken cancellationToken = default)
    {
        string[] csFiles = Directory.GetFiles(srcDirectory, "*.cs", SearchOption.AllDirectories);

        // Regex to find properties whose name starts with a digit
        var propDeclRx = new Regex(@"public\s+([\w<>,\?\[\]]+)\s+([0-9]\w*)");
        // Regex to fix GetFieldDeserializers entries
        var deserRx = new Regex(@"\{\s*""(?<json>_[A-Za-z0-9_]+)""\s*,\s*n\s*=>\s*\{\s*(?<name>[0-9]\w*)");
        // Regex to fix all Write*Value calls in Serialize
        var writeRx = new Regex(
            @"\bWrite(?:StringValue|BoolValue|NumberValue|CollectionOfPrimitiveValues<[^>]+>|CollectionOfObjectValues<[^>]+>|ObjectValue<[^>]+>)" +
            @"\(\s*""(?<json>_[A-Za-z0-9_]+)""\s*,\s*(?<name>[0-9]\w*)\s*\)");

        foreach (string file in csFiles)
        {
            string text = await File.ReadAllTextAsync(file, cancellationToken);
            string original = text;

            // 1) Prefix any public property starting with a digit
            text = propDeclRx.Replace(text, m => $"public {m.Groups[1].Value} N{m.Groups[2].Value}");

            // 2) In deserializer, assign into the new name but keep JSON key
            text = deserRx.Replace(text, m => $"{{ \"{m.Groups["json"].Value}\", n => {{ N{m.Groups["name"].Value}");

            // 3) In Serialize, use new name but still write original JSON key
            text = writeRx.Replace(text, m => $"{m.Value.Substring(0, m.Value.IndexOf('(') + 1)}\"{m.Groups["json"].Value}\", N{m.Groups["name"].Value})");

            if (text != original)
                await File.WriteAllTextAsync(file, text, cancellationToken);
        }
    }

    private static async ValueTask FixEnumIdToString(string srcDirectory, CancellationToken cancellationToken = default)
    {
        var rx = new Regex(@"Id\s*=\s*global::[A-Za-z0-9_.]+?\.(?<idMember>\w+);", RegexOptions.Multiline);
        foreach (string file in Directory.GetFiles(srcDirectory, "*.cs", SearchOption.AllDirectories))
        {
            string text = await File.ReadAllTextAsync(file, cancellationToken);
            string replaced = rx.Replace(text, m => $"Id = \"{m.Groups["idMember"].Value.ToLowerInvariant()}\";");
            if (replaced != text)
                await File.WriteAllTextAsync(file, replaced, cancellationToken);
        }
    }

    /// <summary>
    /// Replace Expiry = "Now + 30 minutes"; with a real DateTimeOffset.Now.AddMinutes(30)
    /// </summary>
    private static async ValueTask FixExpiryDefault(string srcDirectory, CancellationToken cancellationToken = default)
    {
        // Matches any Expiry assignment in the parameterless ctor
        var rx = new Regex(@"Expiry\s*=\s*""[^""]*""\s*;", RegexOptions.Multiline);

        foreach (string file in Directory.GetFiles(srcDirectory, "*.cs", SearchOption.AllDirectories))
        {
            string text = await File.ReadAllTextAsync(file, cancellationToken);
            string newText = rx.Replace(text, "Expiry = DateTimeOffset.Now.AddMinutes(30);");
            if (newText != text)
                await File.WriteAllTextAsync(file, newText, cancellationToken);
        }
    }

    private static async ValueTask FixPrimitiveCollectionNullability(string srcDirectory, CancellationToken cancellationToken = default)
    {
        var rx = new Regex(@"GetCollectionOfPrimitiveValues<double\?>\(\)\?\.AsList\(\)\s*is\s*List<double>\s*(?<var>\w+)\)", RegexOptions.Singleline);

        foreach (string file in Directory.GetFiles(srcDirectory, "*.cs", SearchOption.AllDirectories))
        {
            string text = await File.ReadAllTextAsync(file, cancellationToken);
            string newText = rx.Replace(text, m => $"GetCollectionOfPrimitiveValues<double?>()?.AsList() is List<double?> {m.Groups["var"].Value})");

            if (newText != text)
                await File.WriteAllTextAsync(file, newText, cancellationToken);
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

        string gitHubToken = EnvironmentUtil.GetVariableStrict("GH__TOKEN");
        string name = EnvironmentUtil.GetVariableStrict("GIT__NAME");
        string email = EnvironmentUtil.GetVariableStrict("GIT__EMAIL");

        await _gitUtil.CommitAndPush(gitDirectory, "Automated update", gitHubToken, name, email, cancellationToken);
    }
}