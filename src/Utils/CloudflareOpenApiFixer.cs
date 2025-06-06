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

///<inheritdoc cref="ICloudflareOpenApiFixer"/>
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
            await using FileStream stream = File.OpenRead(sourceFilePath);
            var reader = new OpenApiStreamReader();
            var diagnostic = new OpenApiDiagnostic();
            OpenApiDocument? document = reader.Read(stream, out diagnostic);

            if (diagnostic.Errors.Any())
                Console.WriteLine($"Initial parsing errors: {string.Join(", ", diagnostic.Errors.Select(e => e.Message))}");

            RenameConflictingPaths(document);
            RenameInvalidComponentSchemas(document);
            ApplySchemaNormalizations(document, cancellationToken);
            ScrubComponentRefs(document, cancellationToken);
            ExtractInlineSchemas(document, cancellationToken);

            FixAllInlineValueEnums(document);
            InlinePrimitiveComponents(document);

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

    private void InlinePrimitiveComponents(OpenApiDocument document)
    {
        if (document.Components?.Schemas is not IDictionary<string, OpenApiSchema> comps)
            return;

        // 1) Find all schemas that are pure primitives (string/int/boolean/number) with no props/enum/oneOf/etc.
        var primitives = comps
            .Where(kv =>
                !string.IsNullOrWhiteSpace(kv.Value.Type)
                && (kv.Value.Type == "string"
                    || kv.Value.Type == "integer"
                    || kv.Value.Type == "boolean"
                    || kv.Value.Type == "number")
                && (kv.Value.Properties == null || kv.Value.Properties.Count == 0)
                && (kv.Value.Enum == null || kv.Value.Enum.Count == 0)
                && (kv.Value.OneOf == null || kv.Value.OneOf.Count == 0)
                && (kv.Value.AnyOf == null || kv.Value.AnyOf.Count == 0)
                && (kv.Value.AllOf == null || kv.Value.AllOf.Count == 0)
                && kv.Value.Items == null
            )
            .Select(kv => kv.Key)
            .ToList();

        if (!primitives.Any())
            return;

        // 2) For each primitive‐only component, walk every part of the document and replace references
        foreach (string primKey in primitives)
        {
            // Grab the schema node once:
            var primitiveSchema = comps[primKey];

            // Create a clone to inline—so we don't accidentally modify the component later
            var inlineSchema = new OpenApiSchema
            {
                Type = primitiveSchema.Type,
                Format = primitiveSchema.Format,
                Description = primitiveSchema.Description,
                Example = primitiveSchema.Example,
                MaxLength = primitiveSchema.MaxLength,
                // (copy any other constraints you need)
            };

            // Helper that replaces a Reference‐type schema with inlineSchema if the Id matches
            void ReplaceRef(OpenApiSchema? schema)
            {
                if (schema == null) return;
                if (schema.Reference != null && schema.Reference.Type == ReferenceType.Schema && schema.Reference.Id == primKey)
                {
                    // remove the reference entirely, and copy over inlineSchema fields
                    schema.Reference = null;
                    schema.Type = inlineSchema.Type;
                    schema.Format = inlineSchema.Format;
                    schema.Description = inlineSchema.Description;
                    schema.Example = inlineSchema.Example;
                    schema.MaxLength = inlineSchema.MaxLength;
                    // leave any existing Properties/OneOf/etc alone (they should be null for a primitive)
                    // you could also copy over other constraints like Minimum, Maximum, Pattern, etc.
                }

                // dive deeper in anyOf/oneOf/allOf
                if (schema.AllOf != null)
                    foreach (var c in schema.AllOf) ReplaceRef(c);
                if (schema.OneOf != null)
                    foreach (var c in schema.OneOf) ReplaceRef(c);
                if (schema.AnyOf != null)
                    foreach (var c in schema.AnyOf) ReplaceRef(c);

                // properties
                if (schema.Properties != null)
                    foreach (var prop in schema.Properties.Values)
                        ReplaceRef(prop);

                // items (for arrays)
                if (schema.Items != null)
                    ReplaceRef(schema.Items);

                // additionalProperties
                if (schema.AdditionalProperties != null)
                    ReplaceRef(schema.AdditionalProperties);
            }

            // 2a) Walk through Components.RequestBodies, Responses, Parameters, Headers:
            foreach (var rb in document.Components.RequestBodies.Values)
                foreach (var mt in rb.Content.Values)
                    ReplaceRef(mt.Schema);

            foreach (var resp in document.Components.Responses.Values)
                foreach (var mt in resp.Content.Values)
                    ReplaceRef(mt.Schema);

            foreach (var param in document.Components.Parameters.Values)
                ReplaceRef(param.Schema);

            foreach (var header in document.Components.Headers.Values)
                ReplaceRef(header.Schema);

            // 2b) Walk through all Paths → Operations → Parameters / RequestBody / Responses:
            foreach (var pathItem in document.Paths.Values)
            {
                foreach (var op in pathItem.Operations.Values)
                {
                    // parameters
                    if (op.Parameters != null)
                    {
                        foreach (var p in op.Parameters)
                            ReplaceRef(p.Schema);
                    }

                    // request body
                    if (op.RequestBody?.Content != null)
                    {
                        foreach (var mt in op.RequestBody.Content.Values)
                            ReplaceRef(mt.Schema);
                    }

                    // responses
                    foreach (var resp in op.Responses.Values)
                    {
                        if (resp.Content != null)
                        {
                            foreach (var mt in resp.Content.Values)
                                ReplaceRef(mt.Schema);
                        }
                    }
                }
            }

            // 3) Finally remove the component itself
            comps.Remove(primKey);
        }
    }


    /// <summary>
    /// For every component schema that has an inline 'value' object,
    /// if there is a sibling schema named '{SchemaName}_value' that is
    /// actually an enum, replace the inline 'value' with a reference
    /// to that enum schema.
    /// </summary>
    private void FixAllInlineValueEnums(OpenApiDocument document)
    {
        IDictionary<string, OpenApiSchema>? comps = document.Components?.Schemas;
        if (comps == null) return;

        foreach (KeyValuePair<string, OpenApiSchema> kv in comps.ToList())
        {
            string key = kv.Key;
            OpenApiSchema schema = kv.Value;
            OpenApiSchema wrapperSegment = null;

            // A) inline value property
            if (schema.Properties?.ContainsKey("value") == true)
                wrapperSegment = schema;
            // B) allOf wrapper
            else if (schema.AllOf?.Count == 2 && schema.AllOf[1].Properties?.ContainsKey("value") == true)
                wrapperSegment = schema.AllOf[1];
            else
                continue;

            OpenApiSchema? inline = wrapperSegment.Properties["value"];
            if (inline.Enum == null || inline.Enum.Count == 0) continue;

            var enumKey = $"{key}_value";
            if (!comps.ContainsKey(enumKey))
            {
                comps[enumKey] = new OpenApiSchema
                {
                    Type = inline.Type,
                    Title = enumKey,
                    Enum = inline.Enum.ToList()
                };
            }

            wrapperSegment.Properties["value"] = new OpenApiSchema
            {
                Reference = new OpenApiReference
                {
                    Type = ReferenceType.Schema,
                    Id = enumKey
                }
            };
        }
    }

    private void RenameInvalidComponentSchemas(OpenApiDocument document)
    {
        IDictionary<string, OpenApiSchema>? schemas = document.Components?.Schemas;
        if (schemas == null) return;

        var mapping = new Dictionary<string, string>();
        foreach (string key in schemas.Keys.ToList())
        {
            if (!IsValidIdentifier(key))
            {
                string newKey = SanitizeName(key);
                if (string.IsNullOrWhiteSpace(newKey) || schemas.ContainsKey(newKey))
                    newKey = $"{newKey}_{Guid.NewGuid():N}";
                mapping[key] = newKey;
            }
        }

        foreach ((string oldKey, string newKey) in mapping)
        {
            OpenApiSchema schema = schemas[oldKey];
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
        IDictionary<string, OpenApiSchema>? comps = document.Components?.Schemas;
        if (comps != null)
        {
            // Ensure each schema has a Title
            foreach (KeyValuePair<string, OpenApiSchema> kv in comps)
            {
                if (string.IsNullOrWhiteSpace(kv.Value.Title))
                    kv.Value.Title = kv.Key;
            }

            // Union types: explicit object
            foreach (OpenApiSchema schema in comps.Values)
            {
                if (string.IsNullOrWhiteSpace(schema.Type) && (schema.OneOf.Any() || schema.AnyOf.Any() || schema.AllOf.Any()))
                {
                    schema.Type = "object";
                }
            }

            // Add discriminator for oneOf unions
            foreach (KeyValuePair<string, OpenApiSchema> kv in comps.ToList())
            {
                string key = kv.Key;
                OpenApiSchema schema = kv.Value;
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
                    foreach (OpenApiSchema? branch in schema.OneOf)
                    {
                        if (branch.Reference != null)
                        {
                            string? branchId = branch.Reference.Id;
                            schema.Discriminator.Mapping[branchId] = $"#/components/schemas/{branchId}";
                        }
                    }
                }
            }

            foreach (KeyValuePair<string, OpenApiSchema> kv in comps.ToList())
            {
                string key = kv.Key;
                OpenApiSchema schema = kv.Value;
                if (schema.AnyOf.Any())
                {
                    // force object type
                    schema.Type = "object";
                    const string discName = "type";
                    schema.Discriminator ??= new OpenApiDiscriminator
                    {
                        PropertyName = discName,
                        Mapping = new Dictionary<string, string>()
                    };
                    // add the discriminator property if missing
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

                    // map each referenced branch under anyOf
                    foreach (OpenApiSchema? branch in schema.AnyOf)
                    {
                        if (branch.Reference != null)
                        {
                            string branchId = branch.Reference.Id;
                            schema.Discriminator.Mapping[branchId] = $"#/components/schemas/{branchId}";
                        }
                    }
                }
            }

            // Schemas with properties or additionalProperties need explicit object type
            foreach (OpenApiSchema schema in comps.Values)
            {
                bool hasProps = (schema.Properties != null && schema.Properties.Any()) || schema.AdditionalProperties != null ||
                                schema.AdditionalPropertiesAllowed;
                if (hasProps && string.IsNullOrWhiteSpace(schema.Type))
                {
                    schema.Type = "object";
                }
            }
        }

        // Process paths
        var validPaths = new OpenApiPaths();
        foreach ((string? path, OpenApiPathItem? item) in document.Paths)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (item.Operations == null || !item.Operations.Any())
                continue;

            foreach ((OperationType method, OpenApiOperation? operation) in item.Operations)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Responses
                var newResps = new OpenApiResponses();
                foreach ((string? code, OpenApiResponse? resp) in operation.Responses)
                {
                    // Normalize media type keys
                    if (resp.Content != null)
                    {
                        resp.Content = resp.Content.ToDictionary(p => NormalizeMediaType(p.Key), p => p.Value);
                    }

                    ScrubBrokenRefs(resp.Content, document);

                    // Keep only valid content
                    if (resp.Content != null)
                    {
                        Dictionary<string, OpenApiMediaType> valid = resp.Content.Where(p => p.Value.Schema?.Reference != null || !IsMediaEmpty(p.Value))
                                                                         .ToDictionary(p => p.Key, p => p.Value);

                        if (valid.Any())
                        {
                            string status = code.Equals("4xx", StringComparison.OrdinalIgnoreCase) ? "4XX" : code;
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
                    OpenApiRequestBody? rb = operation.RequestBody;
                    if (rb.Content != null)
                    {
                        rb.Content = rb.Content.ToDictionary(p => NormalizeMediaType(p.Key), p => p.Value);
                    }

                    ScrubBrokenRefs(rb.Content, document);
                    Dictionary<string, OpenApiMediaType>? validRb = rb.Content?.Where(p => p.Value.Schema?.Reference != null || !IsMediaEmpty(p.Value))
                                                                      .ToDictionary(p => p.Key, p => p.Value);

                    operation.RequestBody = (validRb != null && validRb.Any())
                        ? new OpenApiRequestBody {Description = rb.Description, Content = validRb}
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
            foreach (KeyValuePair<string, OpenApiSchema> kv in comps)
            {
                OpenApiSchema schema = kv.Value;

                // 1) is this an object with only 'required' names, but no real props/items/oneOf/etc?
                bool onlyHasRequired = schema.Type == "object" && (schema.Properties == null || schema.Properties.Count == 0) && schema.Items == null &&
                                       !schema.AllOf.Any() && !schema.AnyOf.Any() && !schema.OneOf.Any() && schema.AdditionalProperties == null &&
                                       schema.Required?.Any() == true;

                if (onlyHasRequired)
                {
                    // inject a property schema for each required name
                    List<string> reqs = schema.Required.ToList();
                    schema.Properties = reqs.ToDictionary(name => name, _ => new OpenApiSchema {Type = "object"});
                    // leave Required intact
                    // allow extras
                    schema.AdditionalProperties = new OpenApiSchema {Type = "object"};
                    schema.AdditionalPropertiesAllowed = true;
                    continue;
                }

                // 2) truly empty object (no props, no required)
                bool isTrulyEmpty = schema.Type == "object" && (schema.Properties == null || schema.Properties.Count == 0) && schema.Items == null &&
                                    !schema.AllOf.Any() && !schema.AnyOf.Any() && !schema.OneOf.Any() && schema.AdditionalProperties == null;

                if (isTrulyEmpty)
                {
                    schema.Properties = new Dictionary<string, OpenApiSchema>();
                    schema.AdditionalProperties = new OpenApiSchema {Type = "object"};
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
        IDictionary<string, OpenApiSchema>? comps = document.Components?.Schemas;
        if (comps == null) return;

        foreach (OpenApiPathItem? pathItem in document.Paths.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();
            foreach ((OperationType opType, OpenApiOperation? operation) in pathItem.Operations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string opId = operation.OperationId ?? opType.ToString();
                string safeOpId = SanitizeName(opId);

                if (operation.RequestBody?.Content != null)
                {
                    foreach ((string? mediaType, OpenApiMediaType? media) in operation.RequestBody.Content.ToList())
                    {
                        OpenApiSchema? schema = media.Schema;
                        if (schema != null && schema.Reference == null)
                        {
                            var compName = $"{safeOpId}_RequestBody_{SanitizeName(mediaType)}";
                            AddComponentSchema(document, compName, schema);
                            media.Schema = new OpenApiSchema {Reference = new OpenApiReference {Type = ReferenceType.Schema, Id = compName}};
                        }
                    }
                }

                if (operation.Parameters != null)
                {
                    foreach (OpenApiParameter? param in operation.Parameters.ToList())
                    {
                        OpenApiSchema? schema = param.Schema;
                        if (schema != null && schema.Reference == null)
                        {
                            var compName = $"{safeOpId}_Param_{SanitizeName(param.Name)}";
                            AddComponentSchema(document, compName, schema);
                            param.Schema = new OpenApiSchema {Reference = new OpenApiReference {Type = ReferenceType.Schema, Id = compName}};
                        }
                    }
                }

                foreach ((string? statusCode, OpenApiResponse? response) in operation.Responses)
                {
                    if (response.Content != null)
                    {
                        foreach ((string? mediaType, OpenApiMediaType? media) in response.Content.ToList())
                        {
                            OpenApiSchema? schema = media.Schema;
                            if (schema != null && schema.Reference == null)
                            {
                                var compName = $"{safeOpId}_Response_{SanitizeName(statusCode)}_{SanitizeName(mediaType)}";
                                AddComponentSchema(document, compName, schema);
                                media.Schema = new OpenApiSchema {Reference = new OpenApiReference {Type = ReferenceType.Schema, Id = compName}};
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
            if (reference != null && mapping.TryGetValue(reference.Id, out string? newId))
                reference.Id = newId;
        }

        void ScrubAllRefsRename(OpenApiSchema schema)
        {
            if (schema.Reference != null)
                UpdateRef(schema.Reference);
            foreach (OpenApiSchema? s in schema.AllOf) ScrubAllRefsRename(s);
            foreach (OpenApiSchema? s in schema.OneOf) ScrubAllRefsRename(s);
            foreach (OpenApiSchema? s in schema.AnyOf) ScrubAllRefsRename(s);
            if (schema.Properties != null)
                foreach (OpenApiSchema? prop in schema.Properties.Values)
                    ScrubAllRefsRename(prop);
            if (schema.Items != null)
                ScrubAllRefsRename(schema.Items);
            if (schema.AdditionalProperties != null)
                ScrubAllRefsRename(schema.AdditionalProperties);
        }

        foreach (OpenApiSchema? schema in doc.Components.Schemas.Values)
            ScrubAllRefsRename(schema);

        foreach (OpenApiParameter? param in doc.Components.Parameters.Values)
            UpdateRef(param.Reference);
        foreach (OpenApiHeader? header in doc.Components.Headers.Values)
            UpdateRef(header.Reference);

        foreach (OpenApiRequestBody? rb in doc.Components.RequestBodies.Values)
        {
            UpdateRef(rb.Reference);
            foreach (OpenApiMediaType? media in rb.Content.Values)
                UpdateRef(media.Schema.Reference);
        }

        foreach (OpenApiResponse? resp in doc.Components.Responses.Values)
        {
            UpdateRef(resp.Reference);
            foreach (OpenApiMediaType? media in resp.Content.Values)
                UpdateRef(media.Schema.Reference);
        }

        foreach (OpenApiPathItem? pathItem in doc.Paths.Values)
        {
            foreach (OpenApiOperation? operation in pathItem.Operations.Values)
            {
                UpdateRef(operation.RequestBody?.Reference);
                if (operation.RequestBody?.Content != null)
                    foreach (OpenApiMediaType? media in operation.RequestBody.Content.Values)
                        UpdateRef(media.Schema.Reference);

                if (operation.Parameters != null)
                    foreach (OpenApiParameter? p in operation.Parameters)
                    {
                        UpdateRef(p.Reference);
                        UpdateRef(p.Schema.Reference);
                    }

                foreach (OpenApiResponse? response in operation.Responses.Values)
                {
                    UpdateRef(response.Reference);
                    if (response.Content != null)
                        foreach (OpenApiMediaType? media in response.Content.Values)
                            UpdateRef(media.Schema.Reference);
                }
            }
        }
    }

    private static string SanitizeName(string input)
    {
        var sb = new StringBuilder();
        foreach (char c in input)
        {
            if (char.IsLetterOrDigit(c) || c == '_') sb.Append(c);
            else sb.Append('_');
        }

        return sb.ToString();
    }


    private static bool IsValidResponseKey(string key) =>
        key == "default" || key is "1XX" or "2XX" or "3XX" or "4XX" or "5XX" || (int.TryParse(key, out int s) && s >= 100 && s <= 599);

    private static bool IsValidIdentifier(string id) => !string.IsNullOrWhiteSpace(id) && id.All(c => char.IsLetterOrDigit(c) || c == '_' || c == '-');

    private static bool IsValidSchemaReference(OpenApiReference? reference, OpenApiDocument doc)
    {
        if (reference == null || string.IsNullOrWhiteSpace(reference.Id)) return false;
        OpenApiComponents? comps = doc.Components;
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
        foreach (string key in contentDict.Keys.ToList())
        {
            OpenApiMediaType media = contentDict[key];
            OpenApiSchema? schema = media.Schema;
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

        foreach (OpenApiSchema? s in schema.AllOf) ScrubAllRefs(s, doc);
        foreach (OpenApiSchema? s in schema.OneOf) ScrubAllRefs(s, doc);
        foreach (OpenApiSchema? s in schema.AnyOf) ScrubAllRefs(s, doc);
        foreach (OpenApiSchema? s in schema.Properties.Values) ScrubAllRefs(s, doc);
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
            foreach (OpenApiMediaType media in content.Values) PatchSchema(media.Schema);
        }

        foreach (KeyValuePair<string, OpenApiRequestBody> kv in doc.Components.RequestBodies)
        {
            cancellationToken.ThrowIfCancellationRequested();
            PatchContent(kv.Value.Content);
        }

        foreach (KeyValuePair<string, OpenApiResponse> kv in doc.Components.Responses)
        {
            cancellationToken.ThrowIfCancellationRequested();
            PatchContent(kv.Value.Content);
        }

        foreach (KeyValuePair<string, OpenApiParameter> kv in doc.Components.Parameters)
        {
            cancellationToken.ThrowIfCancellationRequested();
            PatchSchema(kv.Value.Schema);
        }

        foreach (KeyValuePair<string, OpenApiHeader> kv in doc.Components.Headers)
        {
            cancellationToken.ThrowIfCancellationRequested();
            PatchSchema(kv.Value.Schema);
        }

        ScrubTopLevelComponentRefs(doc.Components.RequestBodies, doc);
        ScrubTopLevelComponentRefs(doc.Components.Responses, doc);
        ScrubTopLevelComponentRefs(doc.Components.Parameters, doc);
        ScrubTopLevelComponentRefs(doc.Components.Headers, doc);
    }

    private void RenameConflictingPaths(OpenApiDocument doc)
    {
        var newPaths = new OpenApiPaths();
        foreach (KeyValuePair<string, OpenApiPathItem> kvp in doc.Paths)
        {
            string originalPath = kvp.Key;
            string newPath = originalPath;
            if (originalPath.EndsWith("/item", StringComparison.OrdinalIgnoreCase))
                newPath = originalPath.Replace("/item", "/item_static");
            else if (originalPath.Contains("/item/{", StringComparison.OrdinalIgnoreCase))
                newPath = originalPath.Replace("/item", "/item_by_id");
            newPaths.Add(newPath, kvp.Value);
        }

        doc.Paths = newPaths;
    }

    private void ScrubTopLevelComponentRefs<T>(IDictionary<string, T> comps, OpenApiDocument doc) where T : IOpenApiReferenceable
    {
        foreach (KeyValuePair<string, T> entry in comps)
        {
            if (entry.Value.Reference != null && !IsValidSchemaReference(entry.Value.Reference, doc))
                entry.Value.Reference = null;
        }
    }

    private static string NormalizeMediaType(string mediaType)
    {
        if (string.IsNullOrWhiteSpace(mediaType))
            return "application/json";
        string baseType = mediaType.Split(';')[0].Trim();
        if (baseType.Contains('*') || !baseType.Contains('/'))
            return "application/json";
        return baseType;
    }

    private static bool IsMediaEmpty(OpenApiMediaType media)
    {
        var schemaEmpty = media.Schema == null || (string.IsNullOrWhiteSpace(media.Schema.Type) &&
                                                   (media.Schema.Properties == null || !media.Schema.Properties.Any()) && media.Schema.Items == null);
        var hasExample = media.Example != null || (media.Examples != null && media.Examples.Any());
        return schemaEmpty && !hasExample;
    }


    private static bool IsSchemaEmpty(OpenApiSchema schema)
    {
        return schema == null || (string.IsNullOrWhiteSpace(schema.Type) && (schema.Properties == null || !schema.Properties.Any()) && !schema.AllOf.Any() &&
                                  !schema.OneOf.Any() && !schema.AnyOf.Any() && schema.Items == null && (schema.Enum == null || !schema.Enum.Any()) &&
                                  schema.AdditionalProperties == null && !schema.AdditionalPropertiesAllowed);
    }
}