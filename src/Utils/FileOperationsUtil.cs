using Microsoft.Extensions.Logging;
using Soenneker.Extensions.String;
using Soenneker.Extensions.ValueTask;
using Soenneker.Git.Util.Abstract;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.Runners.Cloudflare.OpenApiClient.Utils.Abstract;
using Soenneker.Utils.Dotnet.Abstract;
using Soenneker.Utils.Environment;
using Soenneker.Utils.File.Download.Abstract;
using Soenneker.Utils.FileSync.Abstract;
using Soenneker.Utils.Process.Abstract;
using Soenneker.Utils.Usings.Abstract;
using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

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
    private readonly IFileUtilSync _fileUtilSync;
    private readonly IUsingsUtil _usingsUtil;

    public FileOperationsUtil(ILogger<FileOperationsUtil> logger, IGitUtil gitUtil, IDotnetUtil dotnetUtil, IProcessUtil processUtil,
        IOpenApiFixer openApiFixer, IFileDownloadUtil fileDownloadUtil, IFileUtilSync fileUtilSync, IUsingsUtil usingsUtil)
    {
        _logger = logger;
        _gitUtil = gitUtil;
        _dotnetUtil = dotnetUtil;
        _processUtil = processUtil;
        _openApiFixer = openApiFixer;
        _fileDownloadUtil = fileDownloadUtil;
        _fileUtilSync = fileUtilSync;
        _usingsUtil = usingsUtil;
    }

    public async ValueTask Process(CancellationToken cancellationToken = default)
    {
        string gitDirectory = await _gitUtil.CloneToTempDirectory($"https://github.com/soenneker/{Constants.Library.ToLowerInvariantFast()}",
            cancellationToken: cancellationToken);

        string targetFilePath = Path.Combine(gitDirectory, "openapi.json");

        _fileUtilSync.DeleteIfExists(targetFilePath);

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
        // await InjectEnumPropertyAndMethods(srcDirectory, cancellationToken);

        await FixPrimitiveCollectionNullability(srcDirectory, cancellationToken);

        await FixExpiryDefault(srcDirectory, cancellationToken);
        await FixStringListDefaults(srcDirectory, cancellationToken);

        string projFilePath = Path.Combine(gitDirectory, "src", $"{Constants.Library}.csproj");

        await _dotnetUtil.Restore(projFilePath, cancellationToken: cancellationToken);

        await _usingsUtil.AddMissing(projFilePath, true, 6, cancellationToken);

        // bool successful = await _dotnetUtil.Build(projFilePath, true, "Release", false, cancellationToken: cancellationToken);

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

    private async ValueTask FixEnumIdToString(string srcDirectory, CancellationToken cancellationToken = default)
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

    //    private async ValueTask InjectEnumPropertyAndMethods(string srcDirectory, CancellationToken cancellationToken = default)
    //    {
    //        // 1) Find all generated .cs files
    //        string[] csFiles = Directory.GetFiles(srcDirectory, "*.cs", SearchOption.AllDirectories);

    //        // 2) Regex to detect the constructor that assigns the enum defaults
    //        var ctorEnumRx = new Regex(
    //            @"public\s+(?<cls>\w+)\s*\([^\)]*\)\s*:\s*base\s*\(\)\s*\{\s*(Id\s*=\s*.+;\s*)?Value\s*=\s*(?<enumFull>global::[^\s;]+)\.(?<member>\w+);",
    //            RegexOptions.Singleline);

    //        foreach (string file in csFiles)
    //        {
    //            string text = await File.ReadAllTextAsync(file, cancellationToken);
    //            Match ctorMatch = ctorEnumRx.Match(text);
    //            if (!ctorMatch.Success)
    //                continue;

    //            // Extract class name, enum full name, and member
    //            string className = ctorMatch.Groups["cls"].Value;
    //            string enumFull = ctorMatch.Groups["enumFull"].Value; // e.g. global::…Zones_pseudo_ipv4_value
    //            string enumType = enumFull.Substring("global::".Length); // e.g. Soenneker.Cloudflare…Zones_pseudo_ipv4_value
    //            string member = ctorMatch.Groups["member"].Value; // e.g. Off

    //            // 3) Rewrite Id assignment from enum to string literal:
    //            //    Id = global::…Zones_pseudo_ipv4_id.Pseudo_ipv4;
    //            //  → Id = "pseudo_ipv4";
    //            text = Regex.Replace(text, @"Id\s*=\s*global::[^\s;]+\.(?<idMember>\w+);", m => $"Id = \"{m.Groups["idMember"].Value.ToLowerInvariant()}\";");

    //            // 4) Inject the new enum‐typed Value property (nullable) right inside the class body
    //            var classBraceRx = new Regex($@"(public\s+partial\s+class\s+{Regex.Escape(className)}\b[^\r\n]*\r?\n\s*\{{\r?\n)", RegexOptions.Multiline);
    //            text = classBraceRx.Replace(text, $@"$1        /// <summary>Strongly‐typed enum value</summary>
    //        public new {enumType}? Value {{ get; set; }} = {enumFull}.{member};

    //");

    //            // 5) Replace the stub GetFieldDeserializers() with one that reads the enum
    //            text = Regex.Replace(text,
    //                @"public override IDictionary<string, Action<IParseNode>> GetFieldDeserializers\(\)\s*\{\s*return new Dictionary<string, Action<IParseNode>>\(base.GetFieldDeserializers\(\)\)\s*\{\s*\}\s*;\s*\}",
    //                $@"public override IDictionary<string, Action<IParseNode>> GetFieldDeserializers()
    //        {{
    //            var map = new Dictionary<string, Action<IParseNode>>(base.GetFieldDeserializers());
    //            map[""value""] = n => {{ Value = n.GetEnumValue<{enumType}>(); }};
    //            return map;
    //        }}");

    //            // 6) Patch Serialize() to write out the enum value
    //            text = Regex.Replace(text,
    //                @"public override void Serialize\(ISerializationWriter writer\)\s*\{\s*_ = writer \?\? throw new ArgumentNullException\(nameof\(writer\)\);\s*base.Serialize\(writer\);\s*\}",
    //                $@"public override void Serialize(ISerializationWriter writer)
    //        {{
    //            _ = writer ?? throw new ArgumentNullException(nameof(writer));
    //            base.Serialize(writer);
    //            writer.WriteEnumValue<{enumType}>(""value"", Value);
    //        }}");

    //            // 7) Write the modified code back
    //            await File.WriteAllTextAsync(file, text, cancellationToken);
    //        }
    //    }

    /// <summary>
    /// Replace Expiry = "Now + 30 minutes"; with a real DateTimeOffset.Now.AddMinutes(30)
    /// </summary>
    private async ValueTask FixExpiryDefault(string srcDirectory, CancellationToken cancellationToken = default)
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

    private async ValueTask FixPrimitiveCollectionNullability(string srcDirectory, CancellationToken cancellationToken = default)
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

        await _gitUtil.CommitAndPush(gitDirectory, "soenneker", "jake@soenneker.com", gitHubToken, "Automated update", cancellationToken);
    }
}