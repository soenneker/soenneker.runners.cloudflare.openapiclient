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
            // Ensure each schema has a Title
            foreach (var kv in comps)
            {
                if (string.IsNullOrWhiteSpace(kv.Value.Title))
                    kv.Value.Title = kv.Key;
            }

            // Union types: explicit object
            foreach (var schema in comps.Values)
            {
                if (string.IsNullOrWhiteSpace(schema.Type)
                    && (schema.OneOf.Any() || schema.AnyOf.Any() || schema.AllOf.Any()))
                {
                    schema.Type = "object";
                }
            }

            // Add discriminator for oneOf unions
            foreach (var kv in comps.ToList())
            {
                var key = kv.Key;
                var schema = kv.Value;
                if (schema.OneOf.Any())
                {
                    // ensure object type
                    schema.Type = "object";
                    const string discName = "type";
                    schema.Discriminator ??= new OpenApiDiscriminator
                    {
                        PropertyName = discName,
                        Mapping = new Dictionary<string, string>()
                    };
                    // add discriminator property
                    schema.Properties ??= new Dictionary<string, OpenApiSchema>();
                    if (!schema.Properties.ContainsKey(discName))
                    {
                        schema.Properties[discName] = new OpenApiSchema
                        {
                            Type = "string",
                            Title = discName,
                            Description = "Union discriminator"
                        };
                    }
                    // build mapping
                    foreach (var branch in schema.OneOf)
                    {
                        if (branch.Reference != null)
                        {
                            var branchId = branch.Reference.Id;
                            schema.Discriminator.Mapping[branchId] = $"#/components/schemas/{branchId}";
                        }
                    }
                }
            }

            // Schemas with properties or additionalProperties need explicit object type
            foreach (var schema in comps.Values)
            {
                var hasProps = (schema.Properties != null && schema.Properties.Any())
                               || schema.AdditionalProperties != null
                               || schema.AdditionalPropertiesAllowed;
                if (hasProps && string.IsNullOrWhiteSpace(schema.Type))
                {
                    schema.Type = "object";
                }
            }

            // Discriminator properties for any existing discriminators
            // (retained if present in components)

            // Wrap primitives
            foreach (var key in comps.Keys.ToList())
            {
                var schema = comps[key];
                if ((schema.Type == "string" || schema.Type == "integer" || schema.Type == "number" || schema.Type == "boolean")
                    && (schema.Properties == null || !schema.Properties.Any())
                    && schema.Items == null
                    && !schema.AllOf.Any()
                    && !schema.AnyOf.Any()
                    && !schema.OneOf.Any()
                    && (schema.Enum == null || !schema.Enum.Any())
                    && schema.AdditionalProperties == null)
                {
                    // Wrap primitive into an object with required 'value' property to satisfy Kiota
                    comps[key] = new OpenApiSchema
                    {
                        Type = "object",
                        Title = key,
                        Description = schema.Description,
                        Properties = new Dictionary<string, OpenApiSchema>
                        {
                            ["value"] = schema
                        },
                        Required = new HashSet<string> { "value" }
                    };
                }
            }
        }

        // Process paths
        var validPaths = new OpenApiPaths();
        foreach (var (path, item) in document.Paths)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (item.Operations == null || !item.Operations.Any())
                continue;

            foreach (var (method, operation) in item.Operations)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Responses
                var newResps = new OpenApiResponses();
                foreach (var (code, resp) in operation.Responses)
                {
                    // Normalize media type keys
                    if (resp.Content != null)
                    {
                        resp.Content = resp.Content
                            .ToDictionary(
                                p => NormalizeMediaType(p.Key),
                                p => p.Value);
                    }
                    ScrubBrokenRefs(resp.Content, document);

                    // Keep only valid content
                    if (resp.Content != null)
                    {
                        var valid = resp.Content
                            .Where(p => p.Value.Schema?.Reference != null || !IsMediaEmpty(p.Value))
                            .ToDictionary(p => p.Key, p => p.Value);

                        if (valid.Any())
                        {
                            var status = code.Equals("4xx", StringComparison.OrdinalIgnoreCase) ? "4XX" : code;
                            newResps[status] = new OpenApiResponse
                            {
                                Description = resp.Description,
                                Content = valid
                            };
                        }
                    }
                }
                operation.Responses = newResps.Any() ? newResps : CreateFallbackResponses();

                // RequestBody
                if (operation.RequestBody != null)
                {
                    var rb = operation.RequestBody;
                    if (rb.Content != null)
                    {
                        rb.Content = rb.Content
                            .ToDictionary(
                                p => NormalizeMediaType(p.Key),
                                p => p.Value);
                    }
                    ScrubBrokenRefs(rb.Content, document);
                    var validRb = rb.Content?
                        .Where(p => p.Value.Schema?.Reference != null || !IsMediaEmpty(p.Value))
                        .ToDictionary(p => p.Key, p => p.Value);

                    operation.RequestBody = (validRb != null && validRb.Any())
                        ? new OpenApiRequestBody { Description = rb.Description, Content = validRb }
                        : CreateFallbackRequestBody();
                }
                else
                {
                    operation.RequestBody = CreateFallbackRequestBody();
                }

                // Parameters retained as-is
            }

            validPaths.Add(path, item);
        }

        document.Paths = validPaths;

        if (comps != null)
        {
            foreach (var kv in comps)
            {
                var schema = kv.Value;

                // 1) is this an object with only 'required' names, but no real props/items/oneOf/etc?
                var onlyHasRequired =
                    schema.Type == "object"
                    && (schema.Properties == null || schema.Properties.Count == 0)
                    && schema.Items == null
                    && !schema.AllOf.Any() && !schema.AnyOf.Any() && !schema.OneOf.Any()
                    && schema.AdditionalProperties == null
                    && schema.Required?.Any() == true;

                if (onlyHasRequired)
                {
                    // inject a property schema for each required name
                    var reqs = schema.Required.ToList();
                    schema.Properties = reqs.ToDictionary(
                        name => name,
                        _ => new OpenApiSchema { Type = "object" }
                    );
                    // leave Required intact
                    // allow extras
                    schema.AdditionalProperties = new OpenApiSchema { Type = "object" };
                    schema.AdditionalPropertiesAllowed = true;
                    continue;
                }

                // 2) truly empty object (no props, no required)
                var isTrulyEmpty =
                    schema.Type == "object"
                    && (schema.Properties == null || schema.Properties.Count == 0)
                    && schema.Items == null
                    && !schema.AllOf.Any() && !schema.AnyOf.Any() && !schema.OneOf.Any()
                    && schema.AdditionalProperties == null;

                if (isTrulyEmpty)
                {
                    schema.Properties = new Dictionary<string, OpenApiSchema>();
                    schema.AdditionalProperties = new OpenApiSchema { Type = "object" };
                    schema.AdditionalPropertiesAllowed = true;
                    // clear any stray required
                    schema.Required = new HashSet<string>();
                }
            }
        }
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

    private static string NormalizeMediaType(string mediaType)
    {
        if (string.IsNullOrWhiteSpace(mediaType))
            return "application/json";
        var baseType = mediaType.Split(';')[0].Trim();
        if (baseType.Contains('*') || !baseType.Contains('/'))
            return "application/json";
        return baseType;
    }

    private static bool IsMediaEmpty(OpenApiMediaType media)
    {
        var schemaEmpty = media.Schema == null
                          || (string.IsNullOrWhiteSpace(media.Schema.Type)
                              && (media.Schema.Properties == null || !media.Schema.Properties.Any())
                              && media.Schema.Items == null);
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
