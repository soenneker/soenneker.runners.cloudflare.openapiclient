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
    private readonly HashSet<OpenApiSchema> _visitedSchemas = new();

    public CloudflareOpenApiFixer(ILogger<CloudflareOpenApiFixer> logger)
    {
        _logger = logger;
    }

    public async ValueTask Fix(string sourceFilePath, string targetFilePath, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var stream = File.OpenRead(sourceFilePath);
            var reader = new OpenApiStreamReader();
            var diagnostic = new OpenApiDiagnostic();
            var document = reader.Read(stream, out diagnostic);

            if (diagnostic.Errors.Any())
                Console.WriteLine($"Initial parsing errors: {string.Join(", ", diagnostic.Errors.Select(e => e.Message))}");

            RenameConflictingPaths(document);

            // 1) Fallback empty-object for truly empty schemas
            if (document.Components?.Schemas != null)
            {
                foreach (var entry in document.Components.Schemas.ToList())
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var key = entry.Key;
                    var schema = entry.Value;

                    if (!IsValidIdentifier(key))
                        continue;

                    var hasContent = !string.IsNullOrWhiteSpace(schema.Type)
                                     || (schema.Properties?.Count > 0)
                                     || schema.Items != null
                                     || schema.AllOf.Count > 0
                                     || schema.AnyOf.Count > 0
                                     || schema.OneOf.Count > 0
                                     || (schema.Enum?.Count > 0)
                                     || schema.AdditionalProperties != null
                                     || schema.AdditionalPropertiesAllowed;

                    if (!hasContent)
                    {
                        document.Components.Schemas[key] = new OpenApiSchema
                        {
                            Type = "object",
                            Description = "Fallback empty object for missing schema",
                            AdditionalPropertiesAllowed = true,
                            AdditionalProperties = new OpenApiSchema { Type = "object" }
                        };
                    }
                }

                // Remove invalid identifiers
                var invalid = document.Components.Schemas.Keys
                                .Where(k => !IsValidIdentifier(k)).ToList();
                foreach (var k in invalid)
                    document.Components.Schemas.Remove(k);

                // 2) Union types need explicit object type
                foreach (var kv in document.Components.Schemas)
                {
                    var schema = kv.Value;
                    if (string.IsNullOrWhiteSpace(schema.Type)
                        && (schema.OneOf.Count > 0 || schema.AnyOf.Count > 0 || schema.AllOf.Count > 0))
                    {
                        schema.Type = "object";
                    }
                }

                // 3) Inject discriminator property
                foreach (var kv in document.Components.Schemas)
                {
                    var schema = kv.Value;
                    var disc = schema.Discriminator;
                    if (disc != null && !string.IsNullOrWhiteSpace(disc.PropertyName))
                    {
                        if (schema.Properties == null)
                            schema.Properties = new Dictionary<string, OpenApiSchema>();
                        if (!schema.Properties.ContainsKey(disc.PropertyName))
                        {
                            schema.Properties[disc.PropertyName] = new OpenApiSchema
                            {
                                Type = "string",
                                Description = "Discriminator property"
                            };
                        }
                    }
                }

                // 4) Wrap primitive-only schemas into object with single 'value' property
                foreach (var kv in document.Components.Schemas.ToList())
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var key = kv.Key;
                    var schema = kv.Value;
                    if ((schema.Type == "string" || schema.Type == "integer" || schema.Type == "number" || schema.Type == "boolean")
                        && (schema.Properties == null || schema.Properties.Count == 0)
                        && schema.Items == null
                        && schema.AllOf.Count == 0
                        && schema.AnyOf.Count == 0
                        && schema.OneOf.Count == 0
                        && (schema.Enum == null || schema.Enum.Count == 0)
                        && schema.AdditionalProperties == null)
                    {
                        var primitive = schema;
                        document.Components.Schemas[key] = new OpenApiSchema
                        {
                            Type = "object",
                            Description = primitive.Description,
                            Properties = new Dictionary<string, OpenApiSchema>
                            {
                                ["value"] = primitive
                            }
                        };
                    }
                }
            }

            // Process paths & operations
            var validPaths = new OpenApiPaths();
            foreach (var (path, item) in document.Paths)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (item.Operations == null || !item.Operations.Any()) continue;
                foreach (var op in item.Operations)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    // Responses
                    var newResps = new OpenApiResponses();
                    foreach (var (code, resp) in op.Value.Responses)
                    {
                        if (!IsValidSchemaReference(resp.Reference, document)) continue;
                        ScrubBrokenRefs(resp.Content, document);
                        if (resp.Content != null)
                        {
                            var emptyMedia = resp.Content.Keys.Where(k => IsSchemaEmpty(resp.Content[k].Schema)).ToList();
                            foreach (var k in emptyMedia) resp.Content.Remove(k);
                        }
                        if (resp.Content == null || !resp.Content.Any()) continue;
                        if (string.Equals(code, "4xx", StringComparison.OrdinalIgnoreCase)) newResps["4XX"] = resp;
                        else if (!IsValidResponseKey(code)) continue;
                        else newResps[code] = resp;
                    }
                    op.Value.Responses = newResps.Any()
                        ? newResps
                        : new OpenApiResponses { ["default"] = new OpenApiResponse { Description = "Default" } };

                    // RequestBody
                    var req = op.Value.RequestBody;
                    if (req == null || !IsValidSchemaReference(req.Reference, document)) op.Value.RequestBody = null;
                    else
                    {
                        ScrubBrokenRefs(req.Content, document);
                        if (req.Content != null)
                        {
                            var empty = req.Content.Keys.Where(k => IsSchemaEmpty(req.Content[k].Schema)).ToList();
                            foreach (var k in empty) req.Content.Remove(k);
                        }
                        if (req.Content == null || !req.Content.Any()) op.Value.RequestBody = null;
                    }

                    // Parameters
                    if (op.Value.Parameters != null)
                    {
                        var good = new List<OpenApiParameter>();
                        foreach (var p in op.Value.Parameters)
                        {
                            if (!IsValidSchemaReference(p.Reference, document)) continue;
                            ScrubBrokenRefs(p.Schema, document);
                            good.Add(p);
                        }
                        op.Value.Parameters = good;
                    }
                }
                validPaths.Add(path, item);
            }
            document.Paths = validPaths;
            ScrubComponentRefs(document, cancellationToken);

            // Write out
            await using var outFs = new FileStream(targetFilePath, FileMode.Create);
            await using var tw = new StreamWriter(outFs);
            var jw = new Microsoft.OpenApi.Writers.OpenApiJsonWriter(tw);
            document.SerializeAsV3(jw);
            await tw.FlushAsync(cancellationToken);

            Console.WriteLine("Cleaned OpenAPI spec saved to " + targetFilePath);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("OpenAPI fix was canceled.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during OpenAPI fix");
            Console.WriteLine("CRASH: " + ex);
            throw;
        }
    }

    private static bool IsValidResponseKey(string key)
        => key == "default"
           || key is "1XX" or "2XX" or "3XX" or "4XX" or "5XX"
           || (int.TryParse(key, out var s) && s >= 100 && s <= 599);

    private static bool IsValidIdentifier(string id)
        => !string.IsNullOrWhiteSpace(id) && id.All(c => char.IsLetterOrDigit(c) || c == '_' || c == '-');

    private static bool IsValidSchemaReference(OpenApiReference? reference, OpenApiDocument doc)
    {
        if (reference == null || string.IsNullOrWhiteSpace(reference.Id)) return false;
        var comps = doc.Components;
        return reference.Type switch
        {
            ReferenceType.Schema => comps.Schemas.ContainsKey(reference.Id),
            ReferenceType.RequestBody => comps.RequestBodies.ContainsKey(reference.Id),
            ReferenceType.Response => comps.Responses.ContainsKey(reference.Id),
            ReferenceType.Parameter => comps.Parameters.ContainsKey(reference.Id),
            ReferenceType.Header => comps.Headers.ContainsKey(reference.Id),
            _ => false
        };
    }

    private void ScrubBrokenRefs(IDictionary<string, OpenApiMediaType>? contentDict, OpenApiDocument doc)
    {
        if (contentDict == null) return;
        foreach (var key in contentDict.Keys.ToList())
        {
            var media = contentDict[key];
            var schema = media.Schema;
            if (schema?.Reference != null && !IsValidSchemaReference(schema.Reference, doc))
            {
                schema.Reference = null;
                _logger.LogWarning("Removed broken media-type ref @ {Key}", key);
            }
            ScrubAllRefs(schema, doc);
        }
    }

    private void ScrubBrokenRefs(OpenApiSchema? schema, OpenApiDocument doc)
    {
        if (schema?.Reference != null && !IsValidSchemaReference(schema.Reference, doc))
        {
            schema.Reference = null;
            _logger.LogWarning("Cleared broken ref for schema {Schema}", schema.Title ?? "(no title)");
        }
        ScrubAllRefs(schema, doc);
    }

    private void ScrubAllRefs(OpenApiSchema? schema, OpenApiDocument doc)
    {
        if (schema == null || !_visitedSchemas.Add(schema)) return;
        if (schema.Reference != null && !IsValidSchemaReference(schema.Reference, doc))
        {
            schema.Reference = null;
            _logger.LogWarning("Cleared nested broken ref for schema {Schema}", schema.Title ?? "(no title)");
        }
        foreach (var s in schema.AllOf) ScrubAllRefs(s, doc);
        foreach (var s in schema.OneOf) ScrubAllRefs(s, doc);
        foreach (var s in schema.AnyOf) ScrubAllRefs(s, doc);
        foreach (var s in schema.Properties.Values) ScrubAllRefs(s, doc);
    }

    private void ScrubComponentRefs(OpenApiDocument doc, CancellationToken cancellationToken)
    {
        void PatchSchema(OpenApiSchema? sch)
        {
            if (sch?.Reference != null && !IsValidSchemaReference(sch.Reference, doc))
            {
                sch.Reference = null;
                _logger.LogWarning("Patched invalid component ref {Schema}", sch.Title ?? "(no title)");
            }
        }
        void PatchContent(IDictionary<string, OpenApiMediaType>? content)
        {
            if (content == null) return;
            foreach (var media in content.Values) PatchSchema(media.Schema);
        }
        foreach (var kv in doc.Components.RequestBodies) { cancellationToken.ThrowIfCancellationRequested(); PatchContent(kv.Value.Content); }
        foreach (var kv in doc.Components.Responses) { cancellationToken.ThrowIfCancellationRequested(); PatchContent(kv.Value.Content); }
        foreach (var kv in doc.Components.Parameters) { cancellationToken.ThrowIfCancellationRequested(); PatchSchema(kv.Value.Schema); }
        foreach (var kv in doc.Components.Headers) { cancellationToken.ThrowIfCancellationRequested(); PatchSchema(kv.Value.Schema); }
        ScrubTopLevelComponentRefs(doc.Components.RequestBodies, doc);
        ScrubTopLevelComponentRefs(doc.Components.Responses, doc);
        ScrubTopLevelComponentRefs(doc.Components.Parameters, doc);
        ScrubTopLevelComponentRefs(doc.Components.Headers, doc);
    }

    private void RenameConflictingPaths(OpenApiDocument doc)
    {
        var newPaths = new OpenApiPaths();
        foreach (var kvp in doc.Paths)
        {
            var originalPath = kvp.Key;
            var newPath = originalPath;
            if (originalPath.EndsWith("/item", StringComparison.OrdinalIgnoreCase))
                newPath = originalPath.Replace("/item", "/item_static");
            else if (originalPath.Contains("/item/{", StringComparison.OrdinalIgnoreCase))
                newPath = originalPath.Replace("/item", "/item_by_id");
            newPaths.Add(newPath, kvp.Value);
        }
        doc.Paths = newPaths;
    }

    private void ScrubTopLevelComponentRefs<T>(IDictionary<string, T> comps, OpenApiDocument doc)
        where T : IOpenApiReferenceable
    {
        foreach (var entry in comps)
        {
            if (entry.Value.Reference != null && !IsValidSchemaReference(entry.Value.Reference, doc))
                entry.Value.Reference = null;
        }
    }

    private static bool IsSchemaEmpty(OpenApiSchema schema)
    {
        return schema == null
            || (string.IsNullOrWhiteSpace(schema.Type)
            && (schema.Properties == null || schema.Properties.Count == 0)
            && schema.AllOf.Count == 0
            && schema.OneOf.Count == 0
            && schema.AnyOf.Count == 0
            && schema.Items == null
            && (schema.Enum == null || schema.Enum.Count == 0)
            && schema.AdditionalProperties == null
            && !schema.AdditionalPropertiesAllowed);
    }
}
