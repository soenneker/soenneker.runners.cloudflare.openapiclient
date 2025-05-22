using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Interfaces;
using Microsoft.OpenApi.Models;
using Microsoft.OpenApi.Readers;
using Soenneker.Runners.Cloudflare.OpenApiClient.Utils.Abstract;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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
            RenameInvalidComponentSchemas(document);
            ApplySchemaNormalizations(document, cancellationToken);
            ScrubComponentRefs(document, cancellationToken);
            ExtractInlineSchemas(document, cancellationToken);

            await using var outFs = new FileStream(targetFilePath, FileMode.Create);
            await using var tw = new StreamWriter(outFs);
            var jw = new Microsoft.OpenApi.Writers.OpenApiJsonWriter(tw);
            document.SerializeAsV3(jw);
            await tw.FlushAsync(cancellationToken);

            Console.WriteLine($"Cleaned OpenAPI spec saved to {targetFilePath}");
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("OpenAPI fix was canceled.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during OpenAPI fix");
            Console.WriteLine($"CRASH: {ex}");
            throw;
        }
    }

    private void RenameInvalidComponentSchemas(OpenApiDocument document)
    {
        var schemas = document.Components?.Schemas;
        if (schemas == null) return;

        var mapping = new Dictionary<string, string>();
        foreach (var key in schemas.Keys.ToList())
        {
            if (!IsValidIdentifier(key))
            {
                var newKey = SanitizeName(key);
                if (string.IsNullOrWhiteSpace(newKey) || schemas.ContainsKey(newKey))
                    newKey = $"{newKey}_{Guid.NewGuid():N}";
                mapping[key] = newKey;
            }
        }

        foreach (var (oldKey, newKey) in mapping)
        {
            var schema = schemas[oldKey];
            schemas.Remove(oldKey);
            if (string.IsNullOrWhiteSpace(schema.Title))
                schema.Title = newKey;
            schemas[newKey] = schema;
        }

        if (mapping.Any())
            UpdateAllReferences(document, mapping);
    }

    private void ApplySchemaNormalizations(OpenApiDocument document, CancellationToken cancellationToken)
    {
        var comps = document.Components?.Schemas;
        if (comps != null)
        {
            foreach (var entry in comps.ToList())
            {
                cancellationToken.ThrowIfCancellationRequested();
                var key = entry.Key;
                var schema = entry.Value;
                if (!IsValidIdentifier(key))
                    continue;

                var hasContent = !string.IsNullOrWhiteSpace(schema.Type)
                                 || (schema.Properties?.Count > 0)
                                 || schema.Items != null
                                 || schema.AllOf.Any()
                                 || schema.AnyOf.Any()
                                 || schema.OneOf.Any()
                                 || (schema.Enum?.Count > 0)
                                 || schema.AdditionalProperties != null
                                 || schema.AdditionalPropertiesAllowed;

                if (!hasContent)
                {
                    comps[key] = new OpenApiSchema
                    {
                        Type = "object",
                        Title = key,
                        Description = "Fallback empty object for missing schema",
                        AdditionalPropertiesAllowed = true,
                        AdditionalProperties = new OpenApiSchema { Type = "object" }
                    };
                }
                else if (string.IsNullOrWhiteSpace(schema.Title))
                {
                    schema.Title = key;
                }
            }

            foreach (var schema in comps.Values)
            {
                if (string.IsNullOrWhiteSpace(schema.Type)
                    && (schema.OneOf.Any() || schema.AnyOf.Any() || schema.AllOf.Any()))
                {
                    schema.Type = "object";
                }

                if (schema.Discriminator?.PropertyName is string propName)
                {
                    schema.Properties ??= new Dictionary<string, OpenApiSchema>();
                    if (!schema.Properties.ContainsKey(propName))
                    {
                        schema.Properties[propName] = new OpenApiSchema
                        {
                            Type = "string",
                            Title = propName,
                            Description = "Discriminator property"
                        };
                    }
                }
            }

            foreach (var kv in comps.ToList())
            {
                cancellationToken.ThrowIfCancellationRequested();
                var schema = kv.Value;
                if ((schema.Type == "string" || schema.Type == "integer" || schema.Type == "number" || schema.Type == "boolean")
                    && (schema.Properties == null || !schema.Properties.Any())
                    && schema.Items == null
                    && !schema.AllOf.Any()
                    && !schema.AnyOf.Any()
                    && !schema.OneOf.Any()
                    && (schema.Enum == null || !schema.Enum.Any())
                    && schema.AdditionalProperties == null)
                {
                    comps[kv.Key] = new OpenApiSchema
                    {
                        Type = "object",
                        Title = kv.Key,
                        Description = schema.Description,
                        Properties = new Dictionary<string, OpenApiSchema>
                        {
                            ["value"] = schema
                        }
                    };
                }
            }
        }

        var validPaths = new OpenApiPaths();
        foreach (var (path, item) in document.Paths)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (item.Operations == null || !item.Operations.Any()) continue;
            foreach (var op in item.Operations)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var newResps = new OpenApiResponses();
                foreach (var (code, resp) in op.Value.Responses)
                {
                    if (!IsValidSchemaReference(resp.Reference, document)) continue;
                    ScrubBrokenRefs(resp.Content, document);
                    if (resp.Content != null)
                    {
                        var emptyMedia = resp.Content
                            .Where(pair => IsMediaEmpty(pair.Value))
                            .Select(pair => pair.Key)
                            .ToList();
                        foreach (var k in emptyMedia) resp.Content.Remove(k);
                    }
                    if (resp.Content == null || !resp.Content.Any()) continue;
                    if (string.Equals(code, "4xx", StringComparison.OrdinalIgnoreCase)) newResps["4XX"] = resp;
                    else if (!IsValidResponseKey(code)) continue;
                    else newResps[code] = resp;
                }
                op.Value.Responses = newResps.Any() ? newResps : CreateFallbackResponses();

                var req = op.Value.RequestBody;
                if (req == null || !IsValidSchemaReference(req.Reference, document))
                {
                    op.Value.RequestBody = CreateFallbackRequestBody();
                }
                else
                {
                    ScrubBrokenRefs(req.Content, document);
                    if (req.Content != null)
                    {
                        var empty = req.Content
                            .Where(pair => IsMediaEmpty(pair.Value))
                            .Select(pair => pair.Key).ToList();
                        foreach (var k in empty) req.Content.Remove(k);
                    }
                    if (req.Content == null || !req.Content.Any())
                    {
                        op.Value.RequestBody = CreateFallbackRequestBody();
                    }
                }
                // Parameters unchanged
            }
            validPaths.Add(path, item);
        }
        document.Paths = validPaths;
    }

    private static OpenApiResponses CreateFallbackResponses()
    {
        return new OpenApiResponses
        {
            ["default"] = new OpenApiResponse
            {
                Description = "Default",
                Content = new Dictionary<string, OpenApiMediaType>
                {
                    ["application/json"] = new OpenApiMediaType
                    {
                        Schema = new OpenApiSchema
                        {
                            Type = "object",
                            Title = "DefaultResponse",
                            Description = "Default response schema"
                        }
                    }
                }
            }
        };
    }

    private static OpenApiRequestBody CreateFallbackRequestBody()
    {
        return new OpenApiRequestBody
        {
            Description = "Fallback request body",
            Content = new Dictionary<string, OpenApiMediaType>
            {
                ["application/json"] = new OpenApiMediaType
                {
                    Schema = new OpenApiSchema
                    {
                        Type = "object",
                        Title = "FallbackRequestBody",
                        Description = "Fallback request body schema"
                    }
                }
            }
        };
    }

    private static void AddComponentSchema(OpenApiDocument doc, string compName, OpenApiSchema schema)
    {
        if (!doc.Components.Schemas.ContainsKey(compName))
        {
            if (string.IsNullOrWhiteSpace(schema.Title))
                schema.Title = compName;
            doc.Components.Schemas[compName] = schema;
        }
    }

    private void ExtractInlineSchemas(OpenApiDocument document, CancellationToken cancellationToken)
    {
        var comps = document.Components?.Schemas;
        if (comps == null) return;

        foreach (var pathItem in document.Paths.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();
            foreach (var (opType, operation) in pathItem.Operations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var opId = operation.OperationId ?? opType.ToString();
                var safeOpId = SanitizeName(opId);

                if (operation.RequestBody?.Content != null)
                {
                    foreach (var (mediaType, media) in operation.RequestBody.Content.ToList())
                    {
                        var schema = media.Schema;
                        if (schema != null && schema.Reference == null)
                        {
                            var compName = $"{safeOpId}_RequestBody_{SanitizeName(mediaType)}";
                            AddComponentSchema(document, compName, schema);
                            media.Schema = new OpenApiSchema { Reference = new OpenApiReference { Type = ReferenceType.Schema, Id = compName } };
                        }
                    }
                }

                if (operation.Parameters != null)
                {
                    foreach (var param in operation.Parameters.ToList())
                    {
                        var schema = param.Schema;
                        if (schema != null && schema.Reference == null)
                        {
                            var compName = $"{safeOpId}_Param_{SanitizeName(param.Name)}";
                            AddComponentSchema(document, compName, schema);
                            param.Schema = new OpenApiSchema { Reference = new OpenApiReference { Type = ReferenceType.Schema, Id = compName } };
                        }
                    }
                }

                foreach (var (statusCode, response) in operation.Responses)
                {
                    if (response.Content != null)
                    {
                        foreach (var (mediaType, media) in response.Content.ToList())
                        {
                            var schema = media.Schema;
                            if (schema != null && schema.Reference == null)
                            {
                                var compName = $"{safeOpId}_Response_{SanitizeName(statusCode)}_{SanitizeName(mediaType)}";
                                AddComponentSchema(document, compName, schema);
                                media.Schema = new OpenApiSchema { Reference = new OpenApiReference { Type = ReferenceType.Schema, Id = compName } };
                            }
                        }
                    }
                }
            }
        }
    }

    private static void UpdateAllReferences(OpenApiDocument doc, Dictionary<string, string> mapping)
    {
        void UpdateRef(OpenApiReference reference)
        {
            if (reference != null && mapping.TryGetValue(reference.Id, out var newId))
                reference.Id = newId;
        }

        void ScrubAllRefsRename(OpenApiSchema schema)
        {
            if (schema.Reference != null)
                UpdateRef(schema.Reference);
            foreach (var s in schema.AllOf) ScrubAllRefsRename(s);
            foreach (var s in schema.OneOf) ScrubAllRefsRename(s);
            foreach (var s in schema.AnyOf) ScrubAllRefsRename(s);
            if (schema.Properties != null)
                foreach (var prop in schema.Properties.Values) ScrubAllRefsRename(prop);
            if (schema.Items != null)
                ScrubAllRefsRename(schema.Items);
            if (schema.AdditionalProperties != null)
                ScrubAllRefsRename(schema.AdditionalProperties);
        }

        foreach (var schema in doc.Components.Schemas.Values)
            ScrubAllRefsRename(schema);

        foreach (var param in doc.Components.Parameters.Values)
            UpdateRef(param.Reference);
        foreach (var header in doc.Components.Headers.Values)
            UpdateRef(header.Reference);

        foreach (var rb in doc.Components.RequestBodies.Values)
        {
            UpdateRef(rb.Reference);
            foreach (var media in rb.Content.Values)
                UpdateRef(media.Schema.Reference);
        }

        foreach (var resp in doc.Components.Responses.Values)
        {
            UpdateRef(resp.Reference);
            foreach (var media in resp.Content.Values)
                UpdateRef(media.Schema.Reference);
        }

        foreach (var pathItem in doc.Paths.Values)
        {
            foreach (var operation in pathItem.Operations.Values)
            {
                UpdateRef(operation.RequestBody?.Reference);
                if (operation.RequestBody?.Content != null)
                    foreach (var media in operation.RequestBody.Content.Values)
                        UpdateRef(media.Schema.Reference);

                if (operation.Parameters != null)
                    foreach (var p in operation.Parameters)
                    {
                        UpdateRef(p.Reference);
                        UpdateRef(p.Schema.Reference);
                    }

                foreach (var response in operation.Responses.Values)
                {
                    UpdateRef(response.Reference);
                    if (response.Content != null)
                        foreach (var media in response.Content.Values)
                            UpdateRef(media.Schema.Reference);
                }
            }
        }
    }

    private static string SanitizeName(string input)
    {
        var sb = new StringBuilder();
        foreach (var c in input)
        {
            if (char.IsLetterOrDigit(c) || c == '_') sb.Append(c);
            else sb.Append('_');
        }
        return sb.ToString();
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

    private static bool IsMediaEmpty(OpenApiMediaType media)
    {
        var schemaEmpty = IsSchemaEmpty(media.Schema);
        var hasExample = media.Example != null || (media.Examples != null && media.Examples.Any());
        return schemaEmpty && !hasExample;
    }

    private static bool IsSchemaEmpty(OpenApiSchema schema)
    {
        return schema == null
               || (string.IsNullOrWhiteSpace(schema.Type)
                   && (schema.Properties == null || !schema.Properties.Any())
                   && !schema.AllOf.Any()
                   && !schema.OneOf.Any()
                   && !schema.AnyOf.Any()
                   && schema.Items == null
                   && (schema.Enum == null || !schema.Enum.Any())
                   && schema.AdditionalProperties == null
                   && !schema.AdditionalPropertiesAllowed);
    }
}
