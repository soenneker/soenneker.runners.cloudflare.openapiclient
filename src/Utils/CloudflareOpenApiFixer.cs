using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Interfaces;
using Microsoft.OpenApi.Models;
using Microsoft.OpenApi.Readers;
using Soenneker.Runners.Cloudflare.OpenApiClient.Utils.Abstract;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Runners.Cloudflare.OpenApiClient.Utils;

public class CloudflareOpenApiFixer : ICloudflareOpenApiFixer
{
    private readonly ILogger<CloudflareOpenApiFixer> _logger;

    private readonly HashSet<OpenApiSchema> _visitedSchemas = [];

    public CloudflareOpenApiFixer(ILogger<CloudflareOpenApiFixer> logger)
    {
        _logger = logger;
    }

    public async ValueTask Fix(string sourceFilePath, string targetFilePath, CancellationToken cancellationToken = default)
    {
        try
        {
            await using FileStream stream = File.OpenRead(sourceFilePath);
            var reader = new OpenApiStreamReader();
            var diagnostic = new OpenApiDiagnostic();
            OpenApiDocument? document = reader.Read(stream, out diagnostic);

            if (diagnostic.Errors.Any())
                Console.WriteLine($"Initial parsing errors: {string.Join(", ", diagnostic.Errors.Select(e => e.Message))}");

            RenameConflictingPaths(document);

            // Clean schemas
            var validSchemas = new Dictionary<string, OpenApiSchema>();
            if (document.Components?.Schemas != null)
            {
                foreach (KeyValuePair<string, OpenApiSchema> schema in document.Components.Schemas)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    OpenApiSchema value = schema.Value;
                    bool hasContent = !string.IsNullOrWhiteSpace(value.Type) || (value.Properties?.Count > 0) ||
                                      value.AllOf.Count > 0 || value.AnyOf.Count > 0 || value.OneOf.Count > 0;

                    if (!hasContent)
                    {
                        validSchemas[schema.Key] = new OpenApiSchema
                        {
                            Type = "object",
                            Description = "Replaced invalid or empty original schema"
                        };
                        continue;
                    }

                    validSchemas.Add(schema.Key, value);
                }

                document.Components.Schemas = validSchemas;
            }

            // Clean paths and scrub bad $refs
            var validPaths = new OpenApiPaths();
            foreach (KeyValuePair<string, OpenApiPathItem> kvp in document.Paths)
            {
                cancellationToken.ThrowIfCancellationRequested();

                string path = kvp.Key;
                OpenApiPathItem item = kvp.Value;

                if (item.Operations == null || !item.Operations.Any())
                    continue;

                foreach (KeyValuePair<OperationType, OpenApiOperation> operation in item.Operations)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var updatedResponses = new OpenApiResponses();
                    foreach (KeyValuePair<string, OpenApiResponse> response in operation.Value.Responses)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        string statusCode = response.Key;
                        OpenApiResponse resp = response.Value;

                        if (!IsValidSchemaReference(resp.Reference, document))
                        {
                            _logger.LogWarning("Removed invalid response $ref in {Path} for status code {Code}", path, statusCode);
                            continue;
                        }

                        ScrubBrokenRefs(resp.Content, document, path, $"response {statusCode}");

                        if (statusCode == "4xx")
                        {
                            updatedResponses["4XX"] = resp;
                        }
                        else if (!IsValidResponseKey(statusCode))
                        {
                            _logger.LogWarning("Removed invalid response key: {Code} in {Path}", statusCode, path);
                            continue;
                        }
                        else
                        {
                            updatedResponses[statusCode] = resp;
                        }
                    }

                    operation.Value.Responses = updatedResponses.Any()
                        ? updatedResponses
                        : new OpenApiResponses
                        {
                            ["default"] = new OpenApiResponse
                            {
                                Description = "Auto-generated default response"
                            }
                        };

                    OpenApiRequestBody? reqBody = operation.Value.RequestBody;
                    if (!IsValidSchemaReference(reqBody?.Reference, document))
                    {
                        _logger.LogWarning("Removed invalid request body ref in {Path}", path);
                        operation.Value.RequestBody = null;
                    }
                    else
                    {
                        ScrubBrokenRefs(reqBody?.Content, document, path, "requestBody");
                    }

                    if (operation.Value.Parameters != null)
                    {
                        var validParams = new List<OpenApiParameter>();
                        foreach (OpenApiParameter? param in operation.Value.Parameters)
                        {
                            cancellationToken.ThrowIfCancellationRequested();

                            if (!IsValidSchemaReference(param.Reference, document))
                            {
                                _logger.LogWarning("Removed invalid parameter ref: {Name} in {Path}", param.Name, path);
                                continue;
                            }

                            ScrubBrokenRefs(param.Schema, document, path, $"parameter {param.Name}");
                            validParams.Add(param);
                        }

                        operation.Value.Parameters = validParams;
                    }
                }

                validPaths.Add(path, item);
            }

            document.Paths = validPaths;

            // Final component scrub
            ScrubComponentRefs(document, cancellationToken);

            // Output
            await using var fileStream = new FileStream(targetFilePath, FileMode.Create, FileAccess.Write, FileShare.None);
            await using var textWriter = new StreamWriter(fileStream);
            var writer = new Microsoft.OpenApi.Writers.OpenApiJsonWriter(textWriter);
            document.SerializeAsV3(writer);
            await textWriter.FlushAsync(cancellationToken);

            Console.WriteLine("Cleaned OpenAPI spec saved to " + targetFilePath);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("OpenAPI fix was canceled.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unhandled exception during OpenAPI fixing");
            Console.WriteLine("CRASH: " + ex);
            throw;
        }
    }

    private static bool IsValidResponseKey(string key)
    {
        return key == "default" || key is "1XX" or "2XX" or "3XX" or "4XX" or "5XX" || int.TryParse(key, out int status) && status is >= 100 and <= 599;
    }

    private static bool IsValidIdentifier(string id)
    {
        return !string.IsNullOrWhiteSpace(id) && id.All(c => char.IsLetterOrDigit(c) || c == '_' || c == '-');
    }

    private static bool IsValidSchemaReference(OpenApiReference? reference, OpenApiDocument document)
    {
        if (reference == null || !IsValidIdentifier(reference.Id))
            return false;

        var components = document.Components;
        return reference.Type switch
        {
            ReferenceType.Schema => components.Schemas.ContainsKey(reference.Id),
            ReferenceType.RequestBody => components.RequestBodies.ContainsKey(reference.Id),
            ReferenceType.Response => components.Responses.ContainsKey(reference.Id),
            ReferenceType.Parameter => components.Parameters.ContainsKey(reference.Id),
            ReferenceType.Header => components.Headers.ContainsKey(reference.Id),
            _ => false
        };
    }

    private void ScrubBrokenRefs(IDictionary<string, OpenApiMediaType>? contentDict, OpenApiDocument document, string path, string context)
    {
        if (contentDict == null)
            return;

        foreach (string key in contentDict.Keys.ToList())
        {
            OpenApiSchema? schema = contentDict[key].Schema;
            if (!IsValidSchemaReference(schema?.Reference, document))
            {
                _logger.LogWarning("Removed invalid content $ref in {Path} ({Context})", path, context);
                contentDict[key].Schema = new OpenApiSchema
                {
                    Type = "object",
                    Description = "Removed invalid $ref"
                };
            }
            else
            {
                ScrubAllRefs(schema, document);
            }
        }
    }

    private void ScrubBrokenRefs(OpenApiSchema? schema, OpenApiDocument document, string path, string context)
    {
        if (schema?.Reference != null && !IsValidSchemaReference(schema.Reference, document))
        {
            _logger.LogWarning("Scrubbed invalid schema $ref in {Path} ({Context})", path, context);
            schema.Reference = null;
            schema.Type = "object";
            schema.Description = "Removed invalid $ref";
        }
        else
        {
            ScrubAllRefs(schema, document);
        }
    }

    private void ScrubAllRefs(OpenApiSchema? schema, OpenApiDocument document)
    {
        if (schema == null || !_visitedSchemas.Add(schema))
            return;

        if (schema.Reference != null && !IsValidSchemaReference(schema.Reference, document))
        {
            schema.Reference = null;
            schema.Type = "object";
            schema.Description = "Removed invalid nested $ref";
        }

        foreach (var s in schema.AllOf) ScrubAllRefs(s, document);
        foreach (var s in schema.OneOf) ScrubAllRefs(s, document);
        foreach (var s in schema.AnyOf) ScrubAllRefs(s, document);
        foreach (var s in schema.Properties.Values) ScrubAllRefs(s, document);
    }

    private void ScrubComponentRefs(OpenApiDocument document, CancellationToken cancellationToken)
    {
        void PatchSchema(OpenApiSchema? schema, string context)
        {
            if (schema?.Reference != null && !IsValidSchemaReference(schema.Reference, document))
            {
                _logger.LogWarning("Removed invalid component $ref ({Context})", context);
                schema.Reference = null;
                schema.Type = "object";
                schema.Description = "Patched invalid $ref";
            }
        }

        void PatchContent(IDictionary<string, OpenApiMediaType>? contentDict, string context)
        {
            if (contentDict == null) return;

            foreach (var content in contentDict.Values)
            {
                PatchSchema(content.Schema, context);
            }
        }

        foreach (var kv in document.Components.RequestBodies)
        {
            cancellationToken.ThrowIfCancellationRequested();
            PatchContent(kv.Value.Content, $"requestBody {kv.Key}");
        }

        foreach (var kv in document.Components.Responses)
        {
            cancellationToken.ThrowIfCancellationRequested();
            PatchContent(kv.Value.Content, $"response {kv.Key}");
        }

        foreach (var kv in document.Components.Parameters)
        {
            cancellationToken.ThrowIfCancellationRequested();
            PatchSchema(kv.Value.Schema, $"parameter {kv.Key}");
        }

        foreach (var kv in document.Components.Headers)
        {
            cancellationToken.ThrowIfCancellationRequested();
            PatchSchema(kv.Value.Schema, $"header {kv.Key}");
        }

        ScrubTopLevelComponentRefs(document.Components.RequestBodies, "requestBodies", document);
        ScrubTopLevelComponentRefs(document.Components.Responses, "responses", document);
        ScrubTopLevelComponentRefs(document.Components.Parameters, "parameters", document);
        ScrubTopLevelComponentRefs(document.Components.Headers, "headers", document);
    }

    private void RenameConflictingPaths(OpenApiDocument document)
    {
        var newPaths = new OpenApiPaths();

        foreach (var kvp in document.Paths)
        {
            string originalPath = kvp.Key;
            string newPath = originalPath;

            if (originalPath.EndsWith("/item"))
            {
                newPath = originalPath.Replace("/item", "/item_static");
            }
            else if (originalPath.Contains("/item/{"))
            {
                newPath = originalPath.Replace("/item", "/item_by_id");
            }

            newPaths.Add(newPath, kvp.Value);
        }

        document.Paths = newPaths;
    }

    private void ScrubTopLevelComponentRefs<T>(IDictionary<string, T> components, string componentType, OpenApiDocument document)
        where T : IOpenApiReferenceable
    {
        foreach (var entry in components)
        {
            var reference = entry.Value.Reference;
            if (!IsValidSchemaReference(reference, document))
            {
                _logger.LogWarning("Scrubbed invalid top-level $ref from {ComponentType} component: {Key}", componentType, entry.Key);
                entry.Value.Reference = null;
            }
        }
    }
}