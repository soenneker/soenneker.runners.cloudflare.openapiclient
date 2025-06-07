using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Any;
using Microsoft.OpenApi.Interfaces;
using Microsoft.OpenApi.Models;
using Microsoft.OpenApi.Readers;
using Soenneker.Runners.Cloudflare.OpenApiClient.Utils.Abstract;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Runners.Cloudflare.OpenApiClient.Utils;

///<inheritdoc cref="ICloudflareOpenApiFixer"/>
public class CloudflareOpenApiFixer : ICloudflareOpenApiFixer
{
    private readonly ILogger<CloudflareOpenApiFixer> _logger;

    public CloudflareOpenApiFixer(ILogger<CloudflareOpenApiFixer> logger)
    {
        _logger = logger;
    }

    public async ValueTask Fix(string sourceFilePath, string targetFilePath, CancellationToken cancellationToken = default)
    {
        try
        {
            await ReadAndValidateOpenApi(sourceFilePath);

            await using MemoryStream pre = PreprocessSpecFile(sourceFilePath);

            var reader = new OpenApiStreamReader();
            var diagnostic = new OpenApiDiagnostic();

            OpenApiDocument? document = reader.Read(pre, out diagnostic);

            if (document == null)
            {
                _logger.LogError("Failed to parse the OpenAPI document. The document object is null, indicating a severe parsing error even after text-level sanitation. Aborting fix.");

                // Log any diagnostic errors from the reader to provide more context.
                if (diagnostic.Errors.Any())
                {
                    var errorMessages = string.Join(Environment.NewLine, diagnostic.Errors.Select(e => $" - {e.Message} (at {e.Pointer})"));
                    _logger.LogError("OpenAPI Reader parsing errors:{NewLine}{Errors}", Environment.NewLine, errorMessages);
                }

                throw new InvalidDataException("The OpenAPI document could not be parsed into a valid object model.");
            }

            SanitizeAllExamples(document);

            EnsureSecuritySchemes(document);

            RenameConflictingPaths(document);
            RenameInvalidComponentSchemas(document);

            // --- STAGE 1: Initial Cleanup ---
            // Run sanitation on existing components. This is still good practice.
            if (document.Components?.Schemas != null)
            {
                foreach (var schema in document.Components.Schemas.Values)
                {
                    RenameInvalidPropertyKeys(schema, new HashSet<OpenApiSchema>());
                }
            }

            // Sanitize inline schemas within operations BEFORE extraction.
            // This is a more direct fix.
            foreach (var path in document.Paths.Values)
            {
                foreach (var op in path.Operations.Values)
                {
                    if (op.RequestBody?.Content != null)
                    {
                        foreach (var media in op.RequestBody.Content.Values)
                        {
                            RenameInvalidPropertyKeys(media.Schema, new HashSet<OpenApiSchema>());
                        }
                    }

                    foreach (var resp in op.Responses.Values)
                    {
                        if (resp.Content != null)
                        {
                            foreach (var media in resp.Content.Values)
                            {
                                RenameInvalidPropertyKeys(media.Schema, new HashSet<OpenApiSchema>());
                            }
                        }
                    }
                }
            }


            InlinePrimitiveComponents(document);
            ApplySchemaNormalizations(document, cancellationToken);
            ScrubComponentRefs(document, cancellationToken);

            // --- STAGE 2: Extraction ---
            // 1) Extract schemas into components
            ExtractInlineSchemas(document, cancellationToken);

            // --- STAGE 3: Post-Extraction Cleanup ---
            // 2) Re-run the property key sanitation on ALL components.
            //    This guarantees that any newly extracted schemas are also cleaned.
            //    This is the most critical change.
            if (document.Components?.Schemas != null)
            {
                _logger.LogInformation("Running post-extraction property key sanitation...");
                foreach (var schema in document.Components.Schemas.Values)
                {
                    RenameInvalidPropertyKeys(schema, new HashSet<OpenApiSchema>());
                }
            }

            // 3) Re‐scrub newly added schemas
            ScrubComponentRefs(document, cancellationToken);

            // 4) Re‐normalize titles on extracted schemas
            ApplySchemaNormalizations(document, cancellationToken);

            _logger.LogInformation("Running deep-clean on all schema metadata (enums, defaults, examples)...");
            if (document.Components?.Schemas != null)
            {
                foreach (var schema in document.Components.Schemas.Values)
                {
                    DeepCleanSchema(schema, new HashSet<OpenApiSchema>());
                }
            }
            // Also clean schemas that might still be inline in parameters
            foreach (var path in document.Paths.Values)
            {
                foreach (var op in path.Operations.Values)
                {
                    if (op.Parameters == null) continue;
                    foreach (var param in op.Parameters)
                    {
                        DeepCleanSchema(param.Schema, new HashSet<OpenApiSchema>());
                    }
                }
            }

            // 6) Remove any empty‐enum branches
            StripEmptyEnumBranches(document);

            // 7) Value‐enum fix & discriminators
            FixAllInlineValueEnums(document);
            StripAllDiscriminators(document);

            await using var outFs = new FileStream(targetFilePath, FileMode.Create);
            await using var tw = new StreamWriter(outFs);
            var jw = new Microsoft.OpenApi.Writers.OpenApiJsonWriter(tw);
            document.SerializeAsV3(jw);

            await tw.FlushAsync(cancellationToken);

            _logger.LogInformation($"Cleaned OpenAPI spec saved to {targetFilePath}");
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("OpenAPI fix was canceled.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during OpenAPI fix");
            _logger.LogInformation($"CRASH: {ex}");
            throw;
        }

        await ReadAndValidateOpenApi(targetFilePath);
    }

    private void DeepCleanSchema(OpenApiSchema? schema, HashSet<OpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema))
        {
            return;
        }

        // 1. Clean default: ""
        if (schema.Default is OpenApiString ds && string.IsNullOrEmpty(ds.Value))
        {
            schema.Default = null;
        }

        // 2. Clean example: ""
        if (schema.Example is OpenApiString es && string.IsNullOrEmpty(es.Value))
        {
            schema.Example = null;
        }

        // 3. THE CRITICAL FIX: Clean the Enum list of any empty or null strings.
        if (schema.Enum != null && schema.Enum.Any())
        {
            var cleanedEnum = schema.Enum
                                    .OfType<OpenApiString>()
                                    .Where(s => !string.IsNullOrEmpty(s.Value)) // Keep only non-empty strings
                                    .Cast<IOpenApiAny>()
                                    .ToList();

            // If the enum list becomes empty after cleaning, remove it entirely.
            schema.Enum = cleanedEnum.Any() ? cleanedEnum : null;
        }

        // 4. Recurse into all possible nested schemas
        if (schema.AllOf != null) foreach (var s in schema.AllOf) DeepCleanSchema(s, visited);
        if (schema.OneOf != null) foreach (var s in schema.OneOf) DeepCleanSchema(s, visited);
        if (schema.AnyOf != null) foreach (var s in schema.AnyOf) DeepCleanSchema(s, visited);
        if (schema.Properties != null) foreach (var p in schema.Properties.Values) DeepCleanSchema(p, visited);
        if (schema.Items != null) DeepCleanSchema(schema.Items, visited);
        if (schema.AdditionalProperties != null) DeepCleanSchema(schema.AdditionalProperties, visited);
    }

    // <summary>
    /// Recursively finds and renames invalid keys within a schema's properties.
    /// This is critical for fixing malformed property names like "" or "invalid_name\"".
    /// </summary>
    private void RenameInvalidPropertyKeys(OpenApiSchema schema, HashSet<OpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema))
            return;

        // 1. Sanitize the properties of the current schema
        if (schema.Properties != null && schema.Properties.Any())
        {
            var keyMapping = new Dictionary<string, string>();
            var originalKeys = schema.Properties.Keys.ToList();

            foreach (var oldKey in originalKeys)
            {
                // Check for invalid keys (empty, whitespace, or containing invalid characters)
                if (string.IsNullOrWhiteSpace(oldKey) || !IsValidIdentifier(oldKey))
                {
                    var newKey = SanitizeName(oldKey);
                    if (string.IsNullOrWhiteSpace(newKey) || schema.Properties.ContainsKey(newKey))
                    {
                        // If sanitizing results in an empty string or a conflict, generate a unique name.
                        newKey = $"prop_{Guid.NewGuid():N}";
                    }

                    keyMapping[oldKey] = newKey;
                    _logger.LogWarning("Sanitizing invalid property key '{OldKey}' to '{NewKey}' in schema '{SchemaTitle}'.", oldKey, newKey,
                        schema.Title ?? "N/A");
                }
            }

            if (keyMapping.Any())
            {
                // Rebuild the properties dictionary with the new keys
                var newProperties = new Dictionary<string, OpenApiSchema>();
                foreach (var oldKey in originalKeys)
                {
                    var currentKey = keyMapping.TryGetValue(oldKey, out var newKey) ? newKey : oldKey;
                    newProperties[currentKey] = schema.Properties[oldKey];
                }

                schema.Properties = newProperties;

                // IMPORTANT: Update the 'required' list with the new key names
                if (schema.Required != null && schema.Required.Any())
                {
                    var newRequired = new HashSet<string>();
                    foreach (var requiredField in schema.Required)
                    {
                        newRequired.Add(keyMapping.TryGetValue(requiredField, out var newName) ? newName : requiredField);
                    }

                    schema.Required = newRequired;
                }
            }
        }

        // 2. Recurse into all nested schemas
        if (schema.AllOf != null)
            foreach (var s in schema.AllOf)
                RenameInvalidPropertyKeys(s, visited);
        if (schema.OneOf != null)
            foreach (var s in schema.OneOf)
                RenameInvalidPropertyKeys(s, visited);
        if (schema.AnyOf != null)
            foreach (var s in schema.AnyOf)
                RenameInvalidPropertyKeys(s, visited);
        if (schema.Properties != null)
            foreach (var p in schema.Properties.Values)
                RenameInvalidPropertyKeys(p, visited);
        if (schema.Items != null) RenameInvalidPropertyKeys(schema.Items, visited);
        if (schema.AdditionalProperties != null) RenameInvalidPropertyKeys(schema.AdditionalProperties, visited);
    }

    /// <summary>
    /// Recursively removes any anyOf/oneOf schemas whose enum list is now empty
    /// (e.g. a branch that only had [""] after your empty-string cleanup).
    /// </summary>
    private void StripEmptyEnumBranches(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null)
            return;

        var visited = new HashSet<OpenApiSchema>();

        void Process(OpenApiSchema schema)
        {
            if (schema == null || !visited.Add(schema))
                return;

            // Clean anyOf
            if (schema.AnyOf != null)
            {
                schema.AnyOf = schema.AnyOf.Where(s => s.Enum == null || s.Enum.Count > 0) // Keep non-enum branches
                                     .ToList();
                foreach (var child in schema.AnyOf)
                    Process(child);
            }

            // Clean oneOf
            if (schema.OneOf != null)
            {
                schema.OneOf = schema.OneOf.Where(s => s.Enum == null || s.Enum.Count > 0) // Keep non-enum branches
                                     .ToList();
                foreach (var child in schema.OneOf)
                    Process(child);
            }

            // Recurse into other nested schemas
            if (schema.AllOf != null)
                foreach (var s in schema.AllOf)
                    Process(s);
            if (schema.Properties != null)
                foreach (var p in schema.Properties.Values)
                    Process(p);
            if (schema.Items != null) Process(schema.Items);
            if (schema.AdditionalProperties != null) Process(schema.AdditionalProperties);
        }

        foreach (var root in document.Components.Schemas.Values)
            Process(root);
    }


    /// <summary>
    /// Strips every Discriminator (and its mappings) from all component schemas
    /// to prevent Kiota from attempting polymorphic inheritance on them.
    /// </summary>
    private void StripAllDiscriminators(OpenApiDocument document)
    {
        var visited = new HashSet<OpenApiSchema>();

        void Strip(OpenApiSchema schema)
        {
            if (schema == null || !visited.Add(schema))
                return;

            schema.Discriminator = null;

            if (schema.AllOf != null)
                foreach (var child in schema.AllOf)
                    Strip(child);
            if (schema.OneOf != null)
                foreach (var child in schema.OneOf)
                    Strip(child);
            if (schema.AnyOf != null)
                foreach (var child in schema.AnyOf)
                    Strip(child);
            if (schema.Properties != null)
                foreach (var child in schema.Properties.Values)
                    Strip(child);
            if (schema.Items != null)
                Strip(schema.Items);
            if (schema.AdditionalProperties != null)
                Strip(schema.AdditionalProperties);
        }

        if (document.Components?.Schemas == null)
            return;

        foreach (var root in document.Components.Schemas.Values)
            Strip(root);
    }

    private MemoryStream PreprocessSpecFile(string path)
    {
        var raw = File.ReadAllText(path, Encoding.UTF8);

        // Your existing ref cleanup. This should run *after* the JSON syntax fix.
        raw = Regex.Replace(raw, @"\{\s*""\$ref""\s*:\s*""(?<id>[^""#/][^""]*)""\s*\}",
            m => $"{{ \"$ref\": \"#/components/schemas/{m.Groups["id"].Value}\" }}");

        return new MemoryStream(Encoding.UTF8.GetBytes(raw));
    }

    private void SanitizeAllExamples(OpenApiDocument document)
    {
        _logger.LogInformation("Starting sanitation of all 'example' values in the document...");

        // 1. Walk through all Paths -> Operations
        foreach (var pathItem in document.Paths.Values)
        {
            foreach (var operation in pathItem.Operations.Values)
            {
                // Clean RequestBody example
                if (operation.RequestBody?.Content != null)
                {
                    foreach (var mediaType in operation.RequestBody.Content.Values)
                    {
                        CleanExampleKeys(mediaType.Example, new HashSet<IOpenApiAny>());
                        if (mediaType.Examples != null)
                        {
                            foreach (var example in mediaType.Examples.Values)
                            {
                                CleanExampleKeys(example.Value, new HashSet<IOpenApiAny>());
                            }
                        }
                    }
                }

                // Clean Response examples
                foreach (var response in operation.Responses.Values)
                {
                    if (response.Content != null)
                    {
                        foreach (var mediaType in response.Content.Values)
                        {
                            CleanExampleKeys(mediaType.Example, new HashSet<IOpenApiAny>());
                            if (mediaType.Examples != null)
                            {
                                foreach (var example in mediaType.Examples.Values)
                                {
                                    CleanExampleKeys(example.Value, new HashSet<IOpenApiAny>());
                                }
                            }
                        }
                    }
                }

                // Clean Parameter examples
                if (operation.Parameters != null)
                {
                    foreach (var parameter in operation.Parameters)
                    {
                        CleanExampleKeys(parameter.Example, new HashSet<IOpenApiAny>());
                        if (parameter.Examples != null)
                        {
                            foreach (var example in parameter.Examples.Values)
                            {
                                CleanExampleKeys(example.Value, new HashSet<IOpenApiAny>());
                            }
                        }
                    }
                }
            }
        }

        // 2. You could extend this to walk through Components as well, if needed.
        // For now, the path-based examples are the most common source of issues.
        _logger.LogInformation("Finished sanitation of 'example' values.");
    }

    private void CleanExampleKeys(IOpenApiAny? current, HashSet<IOpenApiAny> visited)
    {
        if (current == null || !visited.Add(current))
        {
            return;
        }

        if (current is OpenApiObject obj)
        {
            var keyMapping = new Dictionary<string, string>();
            var originalKeys = obj.Keys.ToList();

            foreach (var oldKey in originalKeys)
            {
                if (string.IsNullOrWhiteSpace(oldKey) || !IsValidIdentifier(oldKey))
                {
                    // Sanitize the key using your existing helper
                    var newKey = SanitizeName(oldKey);
                    // Ensure uniqueness if the sanitized key is empty or already exists
                    if (string.IsNullOrWhiteSpace(newKey) || obj.ContainsKey(newKey))
                    {
                        newKey = $"prop_{Guid.NewGuid():N}";
                    }
                    keyMapping[oldKey] = newKey;
                    _logger.LogWarning("Sanitizing invalid key '{OldKey}' to '{NewKey}' in an 'example' object.", oldKey, newKey);
                }
            }

            if (keyMapping.Any())
            {
                // Rebuild the object with the corrected keys
                foreach (var mapping in keyMapping)
                {
                    var value = obj[mapping.Key];
                    obj.Remove(mapping.Key);
                    obj[mapping.Value] = value;
                }
            }

            // After fixing the keys, recurse into the values
            foreach (var value in obj.Values)
            {
                CleanExampleKeys(value, visited);
            }
        }
        else if (current is OpenApiArray arr)
        {
            // Recurse into each item in the array
            foreach (var item in arr)
            {
                CleanExampleKeys(item, visited);
            }
        }
        // Primitives (OpenApiString, OpenApiInteger, etc.) do not need recursion
    }

    private void InlinePrimitiveComponents(OpenApiDocument document)
    {
        if (document.Components?.Schemas is not IDictionary<string, OpenApiSchema> comps)
            return;

        // 1) Find all schemas that are pure primitives (string/int/boolean/number) with no props/enum/oneOf/etc.
        List<string> primitives = comps.Where(kv =>
                                           !string.IsNullOrWhiteSpace(kv.Value.Type) &&
                                           (kv.Value.Type == "string" || kv.Value.Type == "integer" || kv.Value.Type == "boolean" ||
                                            kv.Value.Type == "number") &&
                                           (kv.Value.Properties == null || kv.Value.Properties.Count == 0) &&
                                           (kv.Value.Enum == null || kv.Value.Enum.Count == 0) &&
                                           (kv.Value.OneOf == null || kv.Value.OneOf.Count == 0) && (kv.Value.AnyOf == null || kv.Value.AnyOf.Count == 0) &&
                                           (kv.Value.AllOf == null || kv.Value.AllOf.Count == 0) && kv.Value.Items == null)
                                       .Select(kv => kv.Key)
                                       .ToList();

        if (!primitives.Any())
            return;

        foreach (string primKey in primitives)
        {
            OpenApiSchema primitiveSchema = comps[primKey];

            // Make a shallow “inline” copy of the primitive’s constraints
            var inlineSchema = new OpenApiSchema
            {
                Type = primitiveSchema.Type,
                Format = primitiveSchema.Format,
                Description = primitiveSchema.Description,
                Example = primitiveSchema.Example,
                MaxLength = primitiveSchema.MaxLength,
                Pattern = primitiveSchema.Pattern,
                Minimum = primitiveSchema.Minimum,
                Maximum = primitiveSchema.Maximum,
                // (copy any other primitive constraints you need)
            };

            // We use this HashSet to avoid revisiting the same OpenApiSchema node multiple times
            var visited = new HashSet<OpenApiSchema>();

            void ReplaceRef(OpenApiSchema? schema)
            {
                if (schema == null)
                    return;

                // If we’ve already been here, bail out immediately
                if (!visited.Add(schema))
                    return;

                // 1) If this node is a $ref pointing at primKey, inline it
                if (schema.Reference != null && schema.Reference.Type == ReferenceType.Schema && schema.Reference.Id == primKey)
                {
                    schema.Reference = null;
                    schema.Type = inlineSchema.Type;
                    schema.Format = inlineSchema.Format;
                    schema.Description = inlineSchema.Description;
                    schema.Example = inlineSchema.Example;
                    schema.MaxLength = inlineSchema.MaxLength;
                    schema.Pattern = inlineSchema.Pattern;
                    schema.Minimum = inlineSchema.Minimum;
                    schema.Maximum = inlineSchema.Maximum;
                    // No further recursion needed here
                    return;
                }

                // 2) If it’s a $ref to some OTHER component, fetch that component’s schema and recurse
                if (schema.Reference != null && schema.Reference.Type == ReferenceType.Schema)
                {
                    string? targetId = schema.Reference.Id;
                    if (document.Components.Schemas.TryGetValue(targetId, out OpenApiSchema? targetSchema))
                    {
                        ReplaceRef(targetSchema);
                    }

                    return;
                }

                // 3) Otherwise, descend into children (anyOf/allOf/OneOf → Properties → Items → AdditionalProperties)
                if (schema.AllOf != null)
                    foreach (OpenApiSchema? child in schema.AllOf)
                        ReplaceRef(child);

                if (schema.OneOf != null)
                    foreach (OpenApiSchema? child in schema.OneOf)
                        ReplaceRef(child);

                if (schema.AnyOf != null)
                    foreach (OpenApiSchema? child in schema.AnyOf)
                        ReplaceRef(child);

                if (schema.Properties != null)
                    foreach (OpenApiSchema? prop in schema.Properties.Values)
                        ReplaceRef(prop);

                if (schema.Items != null)
                    ReplaceRef(schema.Items);

                if (schema.AdditionalProperties != null)
                    ReplaceRef(schema.AdditionalProperties);
            }

            // —— NEW: Walk every component schema itself first. 
            //          That way, references buried inside other component definitions get inlined:
            foreach (OpenApiSchema componentSchema in comps.Values.ToList())
            {
                ReplaceRef(componentSchema);
            }

            // 2a) Walk through Component RequestBody → Content → schema
            if (document.Components.RequestBodies != null)
                foreach (OpenApiRequestBody? rb in document.Components.RequestBodies.Values)
                foreach (OpenApiMediaType? mt in rb.Content.Values)
                    ReplaceRef(mt.Schema);

            // 2b) Walk through Component Response → Content → schema
            if (document.Components.Responses != null)
                foreach (OpenApiResponse? resp in document.Components.Responses.Values)
                foreach (OpenApiMediaType? mt in resp.Content.Values)
                    ReplaceRef(mt.Schema);

            // 2c) Walk through Component Parameter → schema
            if (document.Components.Parameters != null)
                foreach (OpenApiParameter? param in document.Components.Parameters.Values)
                    ReplaceRef(param.Schema);

            // 2d) Walk through Component Header → schema
            if (document.Components.Headers != null)
                foreach (OpenApiHeader? header in document.Components.Headers.Values)
                    ReplaceRef(header.Schema);

            // 3) Walk all Paths → Operations → (Parameters → schema), (RequestBody → Content → schema), (Responses → Content → schema)
            foreach (OpenApiPathItem? pathItem in document.Paths.Values)
            {
                foreach (OpenApiOperation? op in pathItem.Operations.Values)
                {
                    if (op.Parameters != null)
                    {
                        foreach (OpenApiParameter? p in op.Parameters)
                            ReplaceRef(p.Schema);
                    }

                    if (op.RequestBody?.Content != null)
                    {
                        foreach (OpenApiMediaType? mt in op.RequestBody.Content.Values)
                            ReplaceRef(mt.Schema);
                    }

                    foreach (OpenApiResponse? resp in op.Responses.Values)
                    {
                        if (resp.Content != null)
                        {
                            foreach (OpenApiMediaType? mt in resp.Content.Values)
                                ReplaceRef(mt.Schema);
                        }
                    }
                }
            }

            // Having inlined every reference to primKey, we can safely remove it from Components.Schemas
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
            OpenApiSchema? wrapperSegment = null;

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

                    schema.Required ??= new HashSet<string>();
                    if (!schema.Required.Contains(discName))
                        schema.Required.Add(discName);

                    // build mapping
                    foreach (OpenApiSchema? branch in schema.OneOf)
                    {
                        if (branch.Reference != null)
                        {
                            string? branchId = branch.Reference.Id;
                            // *** FIX: Ensure the branchId is not null or empty before using it as a key. ***
                            if (!string.IsNullOrWhiteSpace(branchId))
                            {
                                schema.Discriminator.Mapping[branchId] = $"#/components/schemas/{branchId}";
                            }
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

                    schema.Required ??= new HashSet<string>();
                    if (!schema.Required.Contains(discName))
                        schema.Required.Add(discName);

                    // map each referenced branch under anyOf
                    foreach (OpenApiSchema? branch in schema.AnyOf)
                    {
                        if (branch.Reference != null)
                        {
                            string branchId = branch.Reference.Id;
                            // *** FIX: Ensure the branchId is not null or empty before using it as a key. ***
                            if (!string.IsNullOrWhiteSpace(branchId))
                            {
                                schema.Discriminator.Mapping[branchId] = $"#/components/schemas/{branchId}";
                            }
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
                        Dictionary<string, OpenApiMediaType> valid = resp.Content.Where(p =>
                                                                         {
                                                                             OpenApiMediaType? mt = p.Value;
                                                                             OpenApiSchema? sch = mt.Schema;
                                                                             bool hasRef = sch?.Reference != null;
                                                                             bool hasAllOf = sch != null && sch.AllOf.Any();
                                                                             return hasRef || hasAllOf || !IsMediaEmpty(mt);
                                                                         })
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

                if (newResps.Any())
                {
                    EnsureResponseDescriptions(newResps);
                    operation.Responses = newResps;
                }
                else
                {
                    operation.Responses = CreateFallbackResponses();
                }

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
                    // *** FIX: Filter out null/empty strings from required list.
                    List<string> reqs = schema.Required.Where(r => !string.IsNullOrWhiteSpace(r)).ToList();

                    if (reqs.Any())
                    {
                        schema.Properties = reqs.ToDictionary(name => name, _ => new OpenApiSchema {Type = "object"});
                    }

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

    // *** MODIFIED: Added guard for empty component name ***
    private void AddComponentSchema(OpenApiDocument doc, string compName, OpenApiSchema schema)
    {
        if (string.IsNullOrWhiteSpace(compName))
        {
            _logger.LogWarning("Skipped adding a component schema because its generated name was empty.");
            return;
        }

        if (!doc.Components.Schemas.ContainsKey(compName))
        {
            if (string.IsNullOrWhiteSpace(schema.Title))
                schema.Title = compName;
            doc.Components.Schemas[compName] = schema;
        }
    }

    // *** NEW HELPER METHOD ***
    private string GenerateSafePart(string? input, string fallback = "unnamed")
    {
        if (string.IsNullOrWhiteSpace(input))
        {
            return fallback;
        }

        var sanitized = SanitizeName(input);
        return string.IsNullOrWhiteSpace(sanitized) ? fallback : sanitized;
    }

    // *** MODIFIED: Hardened name generation ***
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

                string safeOpId = GenerateSafePart(operation.OperationId, opType.ToString());

                if (operation.RequestBody?.Content != null)
                {
                    foreach ((string? mediaType, OpenApiMediaType? media) in operation.RequestBody.Content.ToList())
                    {
                        OpenApiSchema? schema = media.Schema;
                        if (schema != null && schema.Reference == null)
                        {
                            var safeMediaType = GenerateSafePart(mediaType, "media");
                            var compName = $"{safeOpId}_RequestBody_{safeMediaType}";
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
                            var safeParamName = GenerateSafePart(param.Name, "param");
                            var compName = $"{safeOpId}_Param_{safeParamName}";
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
                                var safeStatusCode = GenerateSafePart(statusCode, "response");
                                var safeMediaType = GenerateSafePart(mediaType, "media");
                                var compName = $"{safeOpId}_Response_{safeStatusCode}_{safeMediaType}";
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
            // Added check for !string.IsNullOrEmpty
            if (reference != null && !string.IsNullOrEmpty(reference.Id) && mapping.TryGetValue(reference.Id, out string? newId))
                reference.Id = newId;
        }

        var visited = new HashSet<OpenApiSchema>();

        void ScrubAllRefsRename(OpenApiSchema schema)
        {
            // Added null check and visited tracking
            if (schema == null || !visited.Add(schema)) return;

            UpdateRef(schema.Reference);
            if (schema.AllOf != null)
                foreach (OpenApiSchema? s in schema.AllOf)
                    ScrubAllRefsRename(s);
            if (schema.OneOf != null)
                foreach (OpenApiSchema? s in schema.OneOf)
                    ScrubAllRefsRename(s);
            if (schema.AnyOf != null)
                foreach (OpenApiSchema? s in schema.AnyOf)
                    ScrubAllRefsRename(s);
            if (schema.Properties != null)
                foreach (OpenApiSchema? prop in schema.Properties.Values)
                    ScrubAllRefsRename(prop);
            if (schema.Items != null)
                ScrubAllRefsRename(schema.Items);
            if (schema.AdditionalProperties != null)
                ScrubAllRefsRename(schema.AdditionalProperties);
        }

        // *** FIX: Add null checks for all component collections ***
        if (doc.Components?.Schemas != null)
            foreach (OpenApiSchema? schema in doc.Components.Schemas.Values)
                ScrubAllRefsRename(schema);

        if (doc.Components?.Parameters != null)
            foreach (OpenApiParameter? param in doc.Components.Parameters.Values)
                // *** FIX: Use null-conditional operator ?. to prevent crash ***
                UpdateRef(param.Schema?.Reference);

        if (doc.Components?.Headers != null)
            foreach (OpenApiHeader? header in doc.Components.Headers.Values)
                UpdateRef(header.Schema?.Reference);

        if (doc.Components?.RequestBodies != null)
            foreach (OpenApiRequestBody? rb in doc.Components.RequestBodies.Values)
            {
                UpdateRef(rb.Reference);
                // *** FIX: Add null check for Content ***
                if (rb.Content != null)
                    foreach (OpenApiMediaType? media in rb.Content.Values)
                        UpdateRef(media.Schema?.Reference);
            }

        if (doc.Components?.Responses != null)
            foreach (OpenApiResponse? resp in doc.Components.Responses.Values)
            {
                UpdateRef(resp.Reference);
                if (resp.Content != null)
                    foreach (OpenApiMediaType? media in resp.Content.Values)
                        UpdateRef(media.Schema?.Reference);
            }

        foreach (OpenApiPathItem? pathItem in doc.Paths.Values)
        {
            foreach (OpenApiOperation? operation in pathItem.Operations.Values)
            {
                UpdateRef(operation.RequestBody?.Reference);
                if (operation.RequestBody?.Content != null)
                    foreach (OpenApiMediaType? media in operation.RequestBody.Content.Values)
                        UpdateRef(media.Schema?.Reference);

                if (operation.Parameters != null)
                    foreach (OpenApiParameter? p in operation.Parameters)
                    {
                        UpdateRef(p.Reference);
                        UpdateRef(p.Schema?.Reference);
                    }

                foreach (OpenApiResponse? response in operation.Responses.Values)
                {
                    UpdateRef(response.Reference);
                    if (response.Content != null)
                        foreach (OpenApiMediaType? media in response.Content.Values)
                            UpdateRef(media.Schema?.Reference);
                }
            }
        }
    }

    private static string SanitizeName(string input)
    {
        if (string.IsNullOrWhiteSpace(input)) return string.Empty;
        var sb = new StringBuilder();
        foreach (char c in input)
        {
            if (char.IsLetterOrDigit(c) || c == '_') sb.Append(c);
            else sb.Append('_');
        }

        return sb.ToString();
    }

    private static bool IsValidIdentifier(string id) => !string.IsNullOrWhiteSpace(id) && id.All(c => char.IsLetterOrDigit(c) || c == '_' || c == '-');

    private static bool IsValidSchemaReference(OpenApiReference? reference, OpenApiDocument doc)
    {
        if (reference == null || string.IsNullOrWhiteSpace(reference.Id)) return false;
        OpenApiComponents? comps = doc.Components;
        if (comps == null) return false;
        return reference.Type switch
        {
            ReferenceType.Schema => comps.Schemas?.ContainsKey(reference.Id) ?? false,
            ReferenceType.RequestBody => comps.RequestBodies?.ContainsKey(reference.Id) ?? false,
            ReferenceType.Response => comps.Responses?.ContainsKey(reference.Id) ?? false,
            ReferenceType.Parameter => comps.Parameters?.ContainsKey(reference.Id) ?? false,
            ReferenceType.Header => comps.Headers?.ContainsKey(reference.Id) ?? false,
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

    /// <summary>
    /// Recursively removes broken $ref links from a schema tree, without infinite recursion.
    /// </summary>
    private void ScrubAllRefs(OpenApiSchema rootSchema, OpenApiDocument doc)
    {
        if (rootSchema == null) return;
        var visited = new HashSet<OpenApiSchema>();

        void Recurse(OpenApiSchema schema)
        {
            if (schema == null || !visited.Add(schema)) return;
            if (schema.Reference != null && !IsValidSchemaReference(schema.Reference, doc))
            {
                schema.Reference = null;
                _logger.LogWarning("Cleared nested broken ref for schema {Schema}", schema.Title ?? "(no title)");
            }

            if (schema.AllOf != null)
                foreach (var s in schema.AllOf)
                    Recurse(s);
            if (schema.OneOf != null)
                foreach (var s in schema.OneOf)
                    Recurse(s);
            if (schema.AnyOf != null)
                foreach (var s in schema.AnyOf)
                    Recurse(s);
            if (schema.Properties != null)
                foreach (var p in schema.Properties.Values)
                    Recurse(p);
            if (schema.Items != null) Recurse(schema.Items);
            if (schema.AdditionalProperties != null) Recurse(schema.AdditionalProperties);
        }

        Recurse(rootSchema);
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

        if (doc.Components == null) return;

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
        // ======================================================================
        // ROBUSTNESS FIX: Add a null check on the Paths property itself.
        // ======================================================================
        if (doc.Paths == null || !doc.Paths.Any())
        {
            _logger.LogInformation("Document contains no paths to process in RenameConflictingPaths. Skipping.");
            return;
        }

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
        if (comps == null) return;
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
        OpenApiSchema? s = media.Schema;
        bool schemaEmpty = s == null || (string.IsNullOrWhiteSpace(s.Type) && (s.Properties == null || !s.Properties.Any()) && s.Items == null &&
                                         !s.AllOf.Any() // ← don’t treat allOf children as “empty”
                                         && !s.AnyOf.Any() && !s.OneOf.Any());
        bool hasExample = s?.Example != null || (media.Examples?.Any() == true);
        return schemaEmpty && !hasExample;
    }


    private static bool IsSchemaEmpty(OpenApiSchema schema)
    {
        return schema == null || (string.IsNullOrWhiteSpace(schema.Type) && (schema.Properties == null || !schema.Properties.Any()) && !schema.AllOf.Any() &&
                                  !schema.OneOf.Any() && !schema.AnyOf.Any() && schema.Items == null && (schema.Enum == null || !schema.Enum.Any()) &&
                                  schema.AdditionalProperties == null && !schema.AdditionalPropertiesAllowed);
    }

    private void EnsureResponseDescriptions(OpenApiResponses responses)
    {
        foreach (var kv in responses)
        {
            var code = kv.Key;
            var resp = kv.Value;
            if (string.IsNullOrWhiteSpace(resp.Description))
            {
                resp.Description = code == "default" ? "Default response" : $"{code} response";
            }
        }
    }

    /// <summary>
    /// Reads an OpenAPI document from the given stream and logs any parsing errors.
    /// </summary>
    /// <param name="filePath">The path to the OpenAPI spec file.</param>
    /// <returns>The diagnostic object containing any parsing errors or warnings.</returns>
    private async ValueTask<OpenApiDiagnostic> ReadAndValidateOpenApi(string filePath)
    {
        await using FileStream stream = File.OpenRead(filePath);

        var reader = new OpenApiStreamReader();
        var diagnostic = new OpenApiDiagnostic();
        OpenApiDocument? document = reader.Read(stream, out diagnostic);

        if (diagnostic.Errors?.Any() == true)
        {
            string msgs = string.Join("; ", diagnostic.Errors.Select(e => e.Message));
            _logger.LogWarning($"OpenAPI parsing errors in {Path.GetFileName(filePath)}: {msgs}");
        }

        return diagnostic;
    }

    private void EnsureSecuritySchemes(OpenApiDocument document)
    {
        if (document.Components == null)
            document.Components = new OpenApiComponents();

        var schemes = document.Components.SecuritySchemes ??= new Dictionary<string, OpenApiSecurityScheme>();

        if (!schemes.ContainsKey("assets_jwt"))
        {
            schemes["assets_jwt"] = new OpenApiSecurityScheme
            {
                Type = SecuritySchemeType.Http,
                Scheme = "bearer",
                BearerFormat = "JWT",
                Description = "JWT used for assets upload"
            };
        }
    }
}