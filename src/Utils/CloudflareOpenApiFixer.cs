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
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Runners.Cloudflare.OpenApiClient.Utils;

///<inheritdoc cref="ICloudflareOpenApiFixer"/>
public sealed class CloudflareOpenApiFixer : ICloudflareOpenApiFixer
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
                _logger.LogError(
                    "Failed to parse the OpenAPI document. The document object is null, indicating a severe parsing error even after text-level sanitation. Aborting fix.");

                // Log any diagnostic errors from the reader to provide more context.
                if (diagnostic.Errors.Any())
                {
                    var errorMessages = string.Join(Environment.NewLine, diagnostic.Errors.Select(e => $" - {e.Message} (at {e.Pointer})"));
                    _logger.LogError("OpenAPI Reader parsing errors:{NewLine}{Errors}", Environment.NewLine, errorMessages);
                }

                throw new InvalidDataException("The OpenAPI document could not be parsed into a valid object model.");
            }

            EnsureSecuritySchemes(document);

            RenameConflictingPaths(document);
            EnsureUniqueOperationIds(document);
            RenameInvalidComponentSchemas(document);
            EnsureUniqueSchemaTitles(document);
            //FixInvalidDefaults(document);


            InlinePrimitiveComponents(document);
            ApplySchemaNormalizations(document, cancellationToken);
            EnsureObjectTypeOnInlineSchemas(document);
            FixEmptyArrayItems(document);
            ScrubComponentRefs(document, cancellationToken);

            // --- STAGE 2: Extraction ---
            // 1) Extract schemas into components
            ExtractInlineSchemas(document, cancellationToken);

            // 3) Re‐scrub newly added schemas
            ScrubComponentRefs(document, cancellationToken);

            // 4) Re‐normalize titles on extracted schemas
            ApplySchemaNormalizations(document, cancellationToken);
            EnsureDiscriminatorPropertyRequired(document);

            _logger.LogInformation("Running deep-clean on all schema metadata (enums, defaults, examples)...");
            if (document.Components?.Schemas != null)
            {
                foreach (var schema in document.Components.Schemas.Values)
                {
                    DeepCleanSchema(schema, new HashSet<OpenApiSchema>());
                }
            }

            NukeAllExamples(document);

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
            // StripAllDiscriminators(document);

            FixSchemaDefaults(document);


            if (document.Components?.Schemas != null)
            {
                foreach (var kvp in document.Components.Schemas.ToList())
                {
                    var compName = kvp.Key;
                    var schema = kvp.Value;

                    // 1) Ensure title
                    if (string.IsNullOrWhiteSpace(schema.Title))
                        schema.Title = compName;

                    // 2) Rename any empty property keys
                    if (schema.Properties != null && schema.Properties.ContainsKey(""))
                    {
                        var fixedProps = new Dictionary<string, OpenApiSchema>();
                        foreach (var prop in schema.Properties)
                        {
                            var name = string.IsNullOrWhiteSpace(prop.Key) ? $"prop_{Guid.NewGuid():N}" : prop.Key;
                            fixedProps[name] = prop.Value;
                        }

                        schema.Properties = fixedProps;
                    }
                }
            }

            // 1) Paths
            CleanEmptyKeysOn(document.Paths, "paths");

            // 2) Component maps
            if (document.Components != null)
            {
                CleanEmptyKeysOn(document.Components.Schemas, "components.schemas");
                CleanEmptyKeysOn(document.Components.Parameters, "components.parameters");
                CleanEmptyKeysOn(document.Components.RequestBodies, "components.requestBodies");
                CleanEmptyKeysOn(document.Components.Responses, "components.responses");
                CleanEmptyKeysOn(document.Components.Headers, "components.headers");
                CleanEmptyKeysOn(document.Components.SecuritySchemes, "components.securitySchemes");

                // 2a) Inside each requestBody/responseBody, strip empty media‐types
                foreach (var rb in document.Components.RequestBodies.Values)
                    CleanEmptyKeysOn(rb.Content, $"requestBodies.{rb.Reference?.Id}.content");
                foreach (var resp in document.Components.Responses.Values)
                    CleanEmptyKeysOn(resp.Content, $"responses.{resp.Reference?.Id}.content");
            }

            // 3) Operations
            foreach (var pathItem in document.Paths.Values)
            {
                foreach (var op in pathItem.Operations.Values)
                {
                    // 3a) Responses
                    CleanEmptyKeysOn(op.Responses, $"operation {op.OperationId}.responses");
                    foreach (var resp in op.Responses.Values)
                        CleanEmptyKeysOn(resp.Content, $"operation {op.OperationId}.responses.content");

                    // 3b) RequestBody media types
                    if (op.RequestBody?.Content != null)
                        CleanEmptyKeysOn(op.RequestBody.Content, $"operation {op.OperationId}.requestBody.content");

                    // 3c) Parameters themselves are already Name-validated, but just in case:
                    var goodParams = op.Parameters?.Where(p => !string.IsNullOrWhiteSpace(p.Name)).ToList();
                    if (goodParams != null) op.Parameters = goodParams;
                }
            }

            if (document.SecurityRequirements != null)
            {
                var cleanGlobals = document.SecurityRequirements.Where(req => req != null && req.Count > 0).ToList();
                document.SecurityRequirements = cleanGlobals.Any() ? cleanGlobals : null;
            }

            // 2) Operation-level security
            foreach (var pathItem in document.Paths.Values)
            {
                foreach (var operation in pathItem.Operations.Values)
                {
                    if (operation.Security != null)
                    {
                        var cleanOps = operation.Security.Where(req => req != null && req.Count > 0).ToList();
                        // if nothing left, null out the property entirely
                        operation.Security = cleanOps.Any() ? cleanOps : null;
                    }
                }
            }

            RemoveEmptyInlineSchemas(document);

            RemoveInvalidDefaults(document);

            CollapseTrivialCompositions(document);

            EnsureTitlesOnCompositionBranches(document);

            HydrateParameterNames(document);

            string json;
            await using (var sw = new StringWriter())
            {
                var scanWriter = new Microsoft.OpenApi.Writers.OpenApiJsonWriter(sw);
                document.SerializeAsV3(scanWriter);     // no cycles → finishes instantly
                scanWriter.Flush();
                json = sw.ToString();
            }

            var emptyPaths = FindEmptyPropertyPaths(json).ToList();
            if (emptyPaths.Any())
            {
                foreach (var p in emptyPaths)
                    _logger.LogError("⚠️  Empty JSON key at {Path}", p);
            }

            var offenders = document.Components.Schemas.Keys
                                    .Where(k => Regex.IsMatch(k, @"(__|--|\.\.)") ||
                                                k.StartsWith("_") || k.EndsWith("_") ||
                                                k.StartsWith(".") || k.EndsWith(".")).ToList();
            _logger.LogWarning("Problematic schema IDs: {Count}", offenders.Count);

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

    private void FixEmptyArrayItems(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null) return;

        var visited = new HashSet<OpenApiSchema>();
        void Walk(OpenApiSchema s)
        {
            if (s == null || !visited.Add(s)) return;

            if (s.Type == "array"
                && s.Items != null
                && IsSchemaEmpty(s.Items))          // your existing helper
            {
                // pick a harmless primitive so Kiota can continue
                s.Items.Type = "string";
                s.Items.Title = $"{s.Title}_Item";
            }

            if (s.Properties != null) foreach (var p in s.Properties.Values) Walk(p);
            Walk(s.Items); Walk(s.AdditionalProperties);
            foreach (var b in s.AllOf ?? Enumerable.Empty<OpenApiSchema>()) Walk(b);
            foreach (var b in s.AnyOf ?? Enumerable.Empty<OpenApiSchema>()) Walk(b);
            foreach (var b in s.OneOf ?? Enumerable.Empty<OpenApiSchema>()) Walk(b);
        }

        foreach (var root in doc.Components.Schemas.Values) Walk(root);
    }

    /// <summary>
    /// Removes “stub” objects (<c>{}</c>) from <c>allOf</c>, <c>oneOf</c>,
    /// and <c>anyOf</c> arrays across the whole document.  
    /// • If the list becomes empty → set it to <c>null</c>.  
    /// • If exactly one branch remains **and it is inline** → merge its
    ///   properties/metadata into the parent and drop the wrapper.
    /// </summary>
    private void CollapseTrivialCompositions(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null) return;

        var visited = new HashSet<OpenApiSchema>();

        foreach (var root in doc.Components.Schemas.Values)
            Visit(root);

        /* ------------------------------------------------------------ */
        void Visit(OpenApiSchema schema)
        /* ------------------------------------------------------------ */
        {
            if (schema == null || !visited.Add(schema)) return;

            // clean each composition list ----------------------------------
            schema.AllOf = CleanList(schema.AllOf, schema);
            schema.OneOf = CleanList(schema.OneOf, schema);
            schema.AnyOf = CleanList(schema.AnyOf, schema);

            // recurse into children ----------------------------------------
            if (schema.Properties != null)
                foreach (var p in schema.Properties.Values) Visit(p);

            if (schema.Items != null) Visit(schema.Items);
            if (schema.AdditionalProperties != null) Visit(schema.AdditionalProperties);
        }

        /* ------------------------------------------------------------ */
        IList<OpenApiSchema>? CleanList(
            IList<OpenApiSchema>? list,
            OpenApiSchema parent)
        /* ------------------------------------------------------------ */
        {
            if (list == null) return null;

            // 1) drop nulls & empty stubs
            var kept = list.Where(s => s != null && !IsSchemaEmpty(s)).ToList();
            if (kept.Count == 0) return null;

            // 2) single survivor
            if (kept.Count == 1 && kept[0].Reference == null)
            {
                MergeIntoParent(kept[0], parent);
                return null;                    // wrapper removed
            }

            // 3) multiple branches – keep the cleaned list
            return kept;
        }

        /* ------------------------------------------------------------ */
        void MergeIntoParent(OpenApiSchema src, OpenApiSchema dst)
        /* ------------------------------------------------------------ */
        {
            // simple scalars
            dst.Type ??= src.Type;
            dst.Format ??= src.Format;
            dst.Description ??= src.Description;
            dst.Nullable |= src.Nullable;
            dst.Example ??= src.Example;

            // enums
            if (src.Enum?.Any() == true)
                dst.Enum = src.Enum;

            // properties
            if (src.Properties?.Any() == true)
            {
                dst.Properties ??= new Dictionary<string, OpenApiSchema>();
                foreach (var kv in src.Properties)
                    dst.Properties[kv.Key] = kv.Value;
            }

            // required list
            if (src.Required?.Any() == true)
            {
                dst.Required ??= new HashSet<string>();
                foreach (var r in src.Required)
                    dst.Required.Add(r);
            }

            // collection helpers
            dst.Items ??= src.Items;
            dst.AdditionalProperties ??= src.AdditionalProperties;
            dst.AdditionalPropertiesAllowed |= src.AdditionalPropertiesAllowed;
        }
    }


    private void EnsureDiscriminatorPropertyRequired(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null) return;

        var visited = new HashSet<OpenApiSchema>();

        void Fix(OpenApiSchema s)
        {
            if (s == null || !visited.Add(s)) return;

            /* ---- 1. if there is a discriminator… ---- */
            var disc = s.Discriminator;
            if (disc != null && !string.IsNullOrWhiteSpace(disc.PropertyName))
            {
                // make sure the property exists
                s.Properties ??= new Dictionary<string, OpenApiSchema>();
                if (!s.Properties.ContainsKey(disc.PropertyName))
                {
                    s.Properties[disc.PropertyName] = new OpenApiSchema
                    {
                        Type = "string",
                        Description = $"Auto-added by fixer – discriminator \"{disc.PropertyName}\"."
                    };
                }

                // and is required
                s.Required ??= new HashSet<string>();
                s.Required.Add(disc.PropertyName);
            }

            /* ---- 2. recurse into every child slot ---- */
            foreach (var p in s.Properties?.Values ?? Enumerable.Empty<OpenApiSchema>()) Fix(p);
            Fix(s.Items);
            Fix(s.AdditionalProperties);

            foreach (var c in s.AllOf ?? Enumerable.Empty<OpenApiSchema>()) Fix(c);
            foreach (var c in s.AnyOf ?? Enumerable.Empty<OpenApiSchema>()) Fix(c);
            foreach (var c in s.OneOf ?? Enumerable.Empty<OpenApiSchema>()) Fix(c);
        }

        foreach (var root in doc.Components.Schemas.Values) Fix(root);
    }

    static IEnumerable<string> FindEmptyPropertyPaths(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var stack = new Stack<string>();
        foreach (var path in Walk(doc.RootElement, "$", stack))
            yield return path;
    }

    static IEnumerable<string> Walk(JsonElement element, string currentPath, Stack<string> stack)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var prop in element.EnumerateObject())
                {
                    // push the property name (even if it’s empty) onto a stack so we can build paths
                    stack.Push(prop.Name);
                    var newPath = $"{currentPath}.{(prop.Name.Length == 0 ? "<EMPTY>" : prop.Name)}";

                    if (prop.Name.Length == 0)
                        yield return newPath;

                    foreach (var p in Walk(prop.Value, newPath, stack))
                        yield return p;

                    stack.Pop();
                }
                break;

            case JsonValueKind.Array:
                int idx = 0;
                foreach (var item in element.EnumerateArray())
                {
                    var newPath = $"{currentPath}[{idx++}]";
                    foreach (var p in Walk(item, newPath, stack))
                        yield return p;
                }
                break;
        }
    }

    private static string TrimQuotes(string value)
    {
        if (value.Length >= 2 && ((value.StartsWith("\"") && value.EndsWith("\"")) || (value.StartsWith("'") && value.EndsWith("'"))))
        {
            return value.Substring(1, value.Length - 2);
        }

        return value;
    }

    /// <summary>
    /// Remove any default values whose CLR type doesn't match the schema.Type.
    /// </summary>
    private void RemoveInvalidDefaults(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null) return;

        foreach (var schema in document.Components.Schemas.Values)
        {
            // If this schema is typed as "object", its Default must be an OpenApiObject
            if (schema.Type == "object" && schema.Default != null && !(schema.Default is OpenApiObject))
            {
                _logger.LogWarning("Removing invalid default ({Default}) from object schema '{Schema}'", schema.Default, schema.Title ?? "(no title)");
                schema.Default = null;
            }
        }
    }

    /// <summary>
    /// Remove any inline schemas that are completely empty (no type, no props, no ref, etc.)
    /// from allOf / oneOf / anyOf arrays throughout the document.
    /// </summary>
    private void RemoveEmptyInlineSchemas(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null)
            return;

        var visited = new HashSet<OpenApiSchema>();
        foreach (var schema in document.Components.Schemas.Values)
            Clean(schema, visited);
    }

    private void Clean(OpenApiSchema schema, HashSet<OpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema))
            return;

        // Filter each composition array
        if (schema.AllOf != null)
        {
            schema.AllOf = schema.AllOf.Where(child => child != null && (child.Reference != null || !IsSchemaEmpty(child))).ToList();
        }

        if (schema.OneOf != null)
        {
            schema.OneOf = schema.OneOf.Where(child => child != null && (child.Reference != null || !IsSchemaEmpty(child))).ToList();
        }

        if (schema.AnyOf != null)
        {
            schema.AnyOf = schema.AnyOf.Where(child => child != null && (child.Reference != null || !IsSchemaEmpty(child))).ToList();
        }

        // Recurse into any remaining branches
        if (schema.AllOf != null)
        {
            foreach (var child in schema.AllOf)
            {
                if (child != null)
                {
                    Clean(child, visited);
                }
            }
        }

        if (schema.OneOf != null)
        {
            foreach (var child in schema.OneOf)
            {
                if (child != null)
                {
                    Clean(child, visited);
                }
            }
        }

        if (schema.AnyOf != null)
        {
            foreach (var child in schema.AnyOf)
            {
                if (child != null)
                {
                    Clean(child, visited);
                }
            }
        }

        // And recurse into normal properties/items
        if (schema.Properties != null)
        {
            foreach (var prop in schema.Properties.Values)
            {
                if (prop != null)
                {
                    Clean(prop, visited);
                }
            }
        }

        if (schema.Items != null)
        {
            Clean(schema.Items, visited);
        }

        if (schema.AdditionalProperties != null)
        {
            Clean(schema.AdditionalProperties, visited);
        }
    }

    /// <summary>
    /// True if this schema has no type, no properties, no enums, no items, no references, etc.
    /// Matches your existing IsSchemaEmpty helper logic.
    /// </summary>
    private static bool IsSchemaEmpty(OpenApiSchema schema)
    {
        if (schema == null) return true;

        return schema.Reference == null && (schema.Properties == null || !schema.Properties.Any()) &&
               schema.Items == null && (schema.AllOf == null || !schema.AllOf.Any()) && (schema.OneOf == null || !schema.OneOf.Any()) &&
               (schema.AnyOf == null || !schema.AnyOf.Any()) && (schema.Enum == null || !schema.Enum.Any()) && schema.AdditionalProperties == null;
    }

    /// <summary>
    /// Performs a definitive and final erasure of all 'example' and 'examples' properties
    /// from the entire OpenAPI document. This method directly targets and iterates through
    /// every known collection that can contain examples, nullifying them at the source.
    /// This is the most direct and reliable approach for this complex document.
    /// </summary>
    /// <param name="document">The OpenAPI document to sanitize.</param>
    private void NukeAllExamples(OpenApiDocument document)
    {
        _logger.LogInformation("--- Starting FINAL, BRUTE-FORCE ERASURE of all examples ---");

        // Use a HashSet to track which schemas have already been nuked to prevent infinite loops.
        var visitedSchemas = new HashSet<OpenApiSchema>();

        // Recursive helper specifically for schemas, which can be deeply nested.
        void NukeSchema(OpenApiSchema? schema)
        {
            if (schema == null || !visitedSchemas.Add(schema))
            {
                return;
            }

            schema.Example = null;

            if (schema.Items != null) NukeSchema(schema.Items);
            if (schema.AdditionalProperties != null) NukeSchema(schema.AdditionalProperties);
            if (schema.Properties != null)
                foreach (var p in schema.Properties.Values)
                    NukeSchema(p);
            if (schema.AllOf != null)
                foreach (var s in schema.AllOf)
                    NukeSchema(s);
            if (schema.AnyOf != null)
                foreach (var s in schema.AnyOf)
                    NukeSchema(s);
            if (schema.OneOf != null)
                foreach (var s in schema.OneOf)
                    NukeSchema(s);
        }

        // 1. Nuke all component definitions
        if (document.Components != null)
        {
            document.Components.Examples = null;

            if (document.Components.Schemas != null)
                foreach (var schema in document.Components.Schemas.Values)
                    NukeSchema(schema);

            if (document.Components.Parameters != null)
                foreach (var p in document.Components.Parameters.Values)
                {
                    p.Example = null;
                    p.Examples = null;
                }

            if (document.Components.Headers != null)
                foreach (var h in document.Components.Headers.Values)
                {
                    h.Example = null;
                    h.Examples = null;
                }

            if (document.Components.RequestBodies != null)
                foreach (var rb in document.Components.RequestBodies.Values)
                    if (rb.Content != null)
                        foreach (var mt in rb.Content.Values)
                        {
                            mt.Example = null;
                            mt.Examples = null;
                        }

            if (document.Components.Responses != null)
                foreach (var resp in document.Components.Responses.Values)
                    if (resp.Content != null)
                        foreach (var mt in resp.Content.Values)
                        {
                            mt.Example = null;
                            mt.Examples = null;
                        }
        }

        // 2. Nuke all path and operation-level definitions
        if (document.Paths != null)
        {
            foreach (var pathItem in document.Paths.Values)
            {
                // Clean parameters directly on the path
                if (pathItem.Parameters != null)
                    foreach (var p in pathItem.Parameters)
                    {
                        p.Example = null;
                        p.Examples = null;
                        NukeSchema(p.Schema);
                    }

                // Clean items within each operation
                foreach (var operation in pathItem.Operations.Values)
                {
                    if (operation.Parameters != null)
                        foreach (var p in operation.Parameters)
                        {
                            p.Example = null;
                            p.Examples = null;
                            NukeSchema(p.Schema);
                        }

                    if (operation.RequestBody?.Content != null)
                        foreach (var mt in operation.RequestBody.Content.Values)
                        {
                            mt.Example = null;
                            mt.Examples = null;
                            NukeSchema(mt.Schema);
                        }

                    if (operation.Responses != null)
                        foreach (var resp in operation.Responses.Values)
                        {
                            if (resp.Headers != null)
                                foreach (var h in resp.Headers.Values)
                                {
                                    h.Example = null;
                                    h.Examples = null;
                                    NukeSchema(h.Schema);
                                }

                            if (resp.Content != null)
                                foreach (var mt in resp.Content.Values)
                                {
                                    mt.Example = null;
                                    mt.Examples = null;
                                    NukeSchema(mt.Schema);
                                }
                        }
                }
            }
        }

        _logger.LogInformation("--- EXAMPLE ERASURE COMPLETE ---");
    }

    private void DeepCleanSchema(OpenApiSchema? schema, HashSet<OpenApiSchema> visited)
    {
        SanitizeExample(schema);

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
            var cleanedEnum = schema.Enum.OfType<OpenApiString>()
                                    .Where(s => !string.IsNullOrEmpty(s.Value)) // Keep only non-empty strings
                                    .Select(s => new OpenApiString(TrimQuotes(s.Value)))
                                    .Cast<IOpenApiAny>()
                                    .ToList();

            // If the enum list becomes empty after cleaning, remove it entirely.
            schema.Enum = cleanedEnum.Any() ? cleanedEnum : null;
        }

        // 4. Recurse into all possible nested schemas
        if (schema.AllOf != null)
            foreach (var s in schema.AllOf)
                DeepCleanSchema(s, visited);
        if (schema.OneOf != null)
            foreach (var s in schema.OneOf)
                DeepCleanSchema(s, visited);
        if (schema.AnyOf != null)
            foreach (var s in schema.AnyOf)
                DeepCleanSchema(s, visited);
        if (schema.Properties != null)
            foreach (var p in schema.Properties.Values)
                DeepCleanSchema(p, visited);
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

            // Make a shallow "inline" copy of the primitive's constraints
            var inlineSchema = new OpenApiSchema
            {
                Type = primitiveSchema.Type,
                Format = primitiveSchema.Format,
                Description = primitiveSchema.Description,
                // Example = primitiveSchema.Example,
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

                // If we've already been here, bail out immediately
                if (!visited.Add(schema))
                    return;

                // 1) If this node is a $ref pointing at primKey, inline it
                if (schema.Reference != null && schema.Reference.Type == ReferenceType.Schema && schema.Reference.Id == primKey)
                {
                    schema.Reference = null;
                    schema.Type = inlineSchema.Type;
                    schema.Format = inlineSchema.Format;
                    schema.Description = inlineSchema.Description;
                    //schema.Example = inlineSchema.Example;
                    schema.MaxLength = inlineSchema.MaxLength;
                    schema.Pattern = inlineSchema.Pattern;
                    schema.Minimum = inlineSchema.Minimum;
                    schema.Maximum = inlineSchema.Maximum;
                    // No further recursion needed here
                    return;
                }

                // 2) If it's a $ref to some OTHER component, fetch that component's schema and recurse
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
        if (document == null) return;
        if (document.Components?.Schemas == null) return;

        var comps = document.Components.Schemas;

        // Ensure each schema has a Title
        foreach (var kv in comps)
        {
            if (kv.Value == null) continue;
            if (string.IsNullOrWhiteSpace(kv.Value.Title))
            {
                kv.Value.Title = kv.Key;
            }
        }

        // Remove empty objects from composition arrays
        var visited = new HashSet<OpenApiSchema>();
        foreach (var schema in comps.Values)
        {
            if (schema == null) continue;
            RemoveEmptyCompositionObjects(schema, visited);
        }

        // Union types: explicit object
        foreach (var schema in comps.Values)
        {
            if (schema == null) continue;

            if (string.Equals(schema.Format, "datetime", StringComparison.OrdinalIgnoreCase))
                schema.Format = "date-time";

            if (string.Equals(schema.Format, "uuid4", StringComparison.OrdinalIgnoreCase))
                schema.Format = "uuid";

            bool hasComposition = (schema.OneOf?.Any() == true) || (schema.AnyOf?.Any() == true) || (schema.AllOf?.Any() == true);

            if (string.IsNullOrWhiteSpace(schema.Type) && hasComposition)
            {
                schema.Type = "object";
            }
        }

        // Add discriminator for oneOf unions
        foreach (var kv in comps.ToList())
        {
            if (kv.Value == null) continue;

            var schema = kv.Value;
            if (schema.OneOf?.Any() != true) continue;

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
            foreach (var branch in schema.OneOf)
            {
                if (branch?.Reference?.Id == null) continue;
                schema.Discriminator.Mapping[branch.Reference.Id] = $"#/components/schemas/{branch.Reference.Id}";
            }
        }

        // Add discriminator for anyOf unions
        foreach (var kv in comps.ToList())
        {
            if (kv.Value == null) continue;

            var schema = kv.Value;
            if (schema.AnyOf?.Any() != true) continue;

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
            foreach (var branch in schema.AnyOf)
            {
                if (branch?.Reference?.Id == null) continue;
                schema.Discriminator.Mapping[branch.Reference.Id] = $"#/components/schemas/{branch.Reference.Id}";
            }
        }

        // Schemas with properties or additionalProperties need explicit object type
        foreach (var schema in comps.Values)
        {
            if (schema == null) continue;

            bool hasProps = (schema.Properties?.Any() == true) || schema.AdditionalProperties != null || schema.AdditionalPropertiesAllowed;

            if (hasProps && string.IsNullOrWhiteSpace(schema.Type))
            {
                schema.Type = "object";
            }
        }

        // Process paths
        var validPaths = new OpenApiPaths();
        foreach (var path in document.Paths)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (path.Value?.Operations == null || !path.Value.Operations.Any())
                continue;

            foreach (var operation in path.Value.Operations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (operation.Value == null) continue;

                // Responses
                var newResps = new OpenApiResponses();
                foreach (var resp in operation.Value.Responses)
                {
                    if (resp.Value == null) continue;

                    if (resp.Value.Reference != null && resp.Value.Reference.Type == ReferenceType.Response)
                    {
                        // just copy the stub; no further processing
                        newResps[resp.Key] = new OpenApiResponse
                        {
                            Reference = resp.Value.Reference
                        };
                        continue;
                    }

                    // Normalize media type keys
                    if (resp.Value.Content != null)
                    {
                        resp.Value.Content = resp.Value.Content.Where(p => p.Key != null && p.Value != null)
                                                 .ToDictionary(p => NormalizeMediaType(p.Key), p => p.Value);
                    }

                    ScrubBrokenRefs(resp.Value.Content, document);

                    // Keep only valid content
                    if (resp.Value.Content != null)
                    {
                        var valid = resp.Value.Content.Where(p =>
                        {
                            if (p.Value == null) return false;
                            var mt = p.Value;
                            if (mt.Schema == null) return false;
                            var sch = mt.Schema;

                            bool hasRef = sch.Reference != null;
                            bool hasAllOf = sch.AllOf?.Any() == true;
                            return hasRef || hasAllOf || !IsMediaEmpty(mt);
                        })
                                        .ToDictionary(p => p.Key, p => p.Value);

                        if (valid.Any())
                        {
                            string status = resp.Key.Equals("4xx", StringComparison.OrdinalIgnoreCase) ? "4XX" : resp.Key;
                            newResps[status] = new OpenApiResponse
                            {
                                Description = resp.Value.Description,
                                Content = valid
                            };
                        }
                    }
                }

                if (newResps.Any())
                {
                    EnsureResponseDescriptions(newResps);
                    operation.Value.Responses = newResps;
                }
                else
                {
                    operation.Value.Responses = CreateFallbackResponses(operation.Key);
                }

                // RequestBody
                if (operation.Value.RequestBody != null)
                {
                    var rb = operation.Value.RequestBody;
                    if (rb.Content != null)
                    {
                        rb.Content = rb.Content.Where(p => p.Key != null && p.Value != null).ToDictionary(p => NormalizeMediaType(p.Key), p => p.Value);
                    }

                    ScrubBrokenRefs(rb.Content, document);
                    var validRb = rb.Content?.Where(p => p.Value?.Schema?.Reference != null || !IsMediaEmpty(p.Value)).ToDictionary(p => p.Key, p => p.Value);

                    operation.Value.RequestBody = (validRb != null && validRb.Any())
                        ? new OpenApiRequestBody { Description = rb.Description, Content = validRb }
                        : CreateFallbackRequestBody();
                }
            }

            validPaths.Add(path.Key, path.Value);
        }

        document.Paths = validPaths;

        // Process remaining schemas
        foreach (var kv in comps)
        {
            if (kv.Value == null) continue;
            var schema = kv.Value;

            // 1) is this an object with only 'required' names, but no real props/items/oneOf/etc?
            bool onlyHasRequired = schema.Type == "object" && (schema.Properties == null || !schema.Properties.Any()) && schema.Items == null &&
                                   (schema.AllOf?.Any() != true) && (schema.AnyOf?.Any() != true) && (schema.OneOf?.Any() != true) &&
                                   schema.AdditionalProperties == null && (schema.Required?.Any() == true);

            if (onlyHasRequired)
            {
                var reqs = schema.Required?.Where(r => !string.IsNullOrWhiteSpace(r)).ToList() ?? new List<string>();

                if (reqs.Any())
                {
                    schema.Properties = reqs.ToDictionary(name => name, _ => new OpenApiSchema { Type = "object" });
                }

                schema.AdditionalProperties = new OpenApiSchema { Type = "object" };
                schema.AdditionalPropertiesAllowed = true;
                continue;
            }

            // 2) truly empty object (no props, no required)
            bool isTrulyEmpty = schema.Type == "object" && (schema.Properties == null || !schema.Properties.Any()) && schema.Items == null &&
                                (schema.AllOf?.Any() != true) && (schema.AnyOf?.Any() != true) && (schema.OneOf?.Any() != true) &&
                                schema.AdditionalProperties == null;

            if (isTrulyEmpty)
            {
                schema.Properties = new Dictionary<string, OpenApiSchema>();
                schema.AdditionalProperties = new OpenApiSchema { Type = "object" };
                schema.AdditionalPropertiesAllowed = true;
                schema.Required = new HashSet<string>();
            }
        }

        // Process enum types
        foreach (var schema in comps.Values)
        {
            if (schema?.Enum == null || !schema.Enum.Any()) continue;

            // if all enum values are strings, force type="string"
            if (schema.Enum.All(x => x is OpenApiString))
            {
                schema.Type = "string";
            }
        }

        // Process nullable types
        var visitedSchemas = new HashSet<OpenApiSchema>();
        foreach (var root in comps.Values)
        {
            if (root == null) continue;
            InjectTypeForNullable(root, visitedSchemas);
        }
    }

    private static OpenApiResponses CreateFallbackResponses(OperationType op)
    {
        var code = CanonicalSuccess(op);

        return new OpenApiResponses
        {
            [code] = new OpenApiResponse
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

        var validatedName = ValidateComponentName(compName);

        if (!doc.Components.Schemas.ContainsKey(validatedName))
        {
            if (string.IsNullOrWhiteSpace(schema.Title))
                schema.Title = validatedName;
            doc.Components.Schemas[validatedName] = schema;
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

    private string ValidateComponentName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            _logger.LogWarning("Component name was empty, using fallback name");
            return "UnnamedComponent";
        }

        // Remove any invalid characters that might cause issues
        var sanitized = Regex.Replace(name, @"[^a-zA-Z0-9_]", "_");

        // Ensure it starts with a letter
        if (!char.IsLetter(sanitized[0]))
        {
            sanitized = "C" + sanitized;
        }

        return sanitized;
    }

    // *** MODIFIED: Hardened name generation ***
    /// <summary>
    /// Walk every path/operation, pull inline request- and response-schemas up
    /// into <c>#/components/schemas</c>, and normalise FastAPI-style parameters
    /// that still use <c>"content": {"*/*": { "schema": …}}</c>.
    /// </summary>
    private void ExtractInlineSchemas(OpenApiDocument document, CancellationToken cancellationToken)
    {
        // Helper: treat simple data envelopes as “already okay” – don’t extract them.
        static bool IsSimpleEnvelope(OpenApiSchema s) =>
            s.Properties?.Count == 1 && s.Properties.TryGetValue("data", out var p) && p?.Reference != null && (s.Required == null || s.Required.Count <= 1);

        IDictionary<string, OpenApiSchema>? comps = document.Components?.Schemas;
        if (comps == null) return;

        foreach (OpenApiPathItem pathItem in document.Paths.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach ((OperationType opType, OpenApiOperation operation) in pathItem.Operations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (operation == null) continue;

                // build a safe prefix for any component names we generate
                string safeOpId = ValidateComponentName(GenerateSafePart(operation.OperationId, opType.ToString()));

                // ─────────────────────────────────────────────────────────────
                // 1) NORMALISE PARAMETERS  (content → schema)
                // ─────────────────────────────────────────────────────────────
                if (operation.Parameters != null)
                {
                    foreach (var param in operation.Parameters.ToList())
                    {
                        if (param.Content?.Any() == true)
                        {
                            var first = param.Content.Values.FirstOrDefault();
                            if (first?.Schema != null)
                                param.Schema = first.Schema;

                            param.Content = null; // Kiota ignores it anyway
                        }
                    }
                }

                // ─────────────────────────────────────────────────────────────
                // 2) REQUEST BODIES
                // ─────────────────────────────────────────────────────────────
                if (operation.RequestBody?.Content != null)
                {
                    foreach ((string mediaType, OpenApiMediaType media) in operation.RequestBody.Content.ToList())
                    {
                        OpenApiSchema? schema = media?.Schema;
                        if (schema == null || schema.Reference != null) continue;
                        if (IsSimpleEnvelope(schema)) continue; // keep simple {data:$ref} inline

                        // string safeMedia = ValidateComponentName(GenerateSafePart(mediaType, "media"));

                        string safeMedia;
                        var subtype = mediaType.Split(';')[0].Split('/').Last(); // "json"
                        if (subtype.Equals("json", StringComparison.OrdinalIgnoreCase))
                            safeMedia = ""; // omit suffix for plain JSON
                        else
                            safeMedia = ValidateComponentName(GenerateSafePart(subtype, "media"));

                        string baseName = $"{safeOpId}";
                        string compName = ReserveUniqueSchemaName(comps, baseName, $"RequestBody_{safeMedia}");

                        AddComponentSchema(document, compName, schema);
                        media.Schema = new OpenApiSchema
                        {
                            Reference = new OpenApiReference
                            {
                                Type = ReferenceType.Schema,
                                Id = compName
                            }
                        };
                    }
                }

                // ─────────────────────────────────────────────────────────────
                // 3) RESPONSES
                // ─────────────────────────────────────────────────────────────
                foreach ((string statusCode, OpenApiResponse response) in operation.Responses)
                {
                    if (response?.Content == null) continue;

                    foreach ((string mediaType, OpenApiMediaType media) in response.Content.ToList())
                    {
                        OpenApiSchema? schema = media?.Schema;
                        if (schema == null || schema.Reference != null) continue;
                        if (IsSimpleEnvelope(schema)) continue;

                        string safeMedia = ValidateComponentName(GenerateSafePart(mediaType, "media"));
                        string baseName = $"{safeOpId}_{statusCode}";
                        string compName = ReserveUniqueSchemaName(comps, baseName, $"Response_{safeMedia}");

                        AddComponentSchema(document, compName, schema);
                        media.Schema = new OpenApiSchema
                        {
                            Reference = new OpenApiReference
                            {
                                Type = ReferenceType.Schema,
                                Id = compName
                            }
                        };
                    }
                }
            }
        }
    }

    private string ReserveUniqueSchemaName(IDictionary<string, OpenApiSchema> comps, string baseName, string fallbackSuffix)
    {
        if (!comps.ContainsKey(baseName))
            return baseName;

        string withSuffix = $"{baseName}_{fallbackSuffix}";
        if (!comps.ContainsKey(withSuffix))
            return withSuffix;

        // final fallback: add numeric counter
        int i = 2;
        string numbered;
        do
        {
            numbered = $"{withSuffix}_{i++}";
        } while (comps.ContainsKey(numbered));

        return numbered;
    }

    private static void EnsureUniqueOperationIds(OpenApiDocument doc)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var path in doc.Paths.Values)
            foreach (var kvp in path.Operations)
            {
                var op = kvp.Value;
                if (string.IsNullOrWhiteSpace(op.OperationId))
                    op.OperationId = $"{kvp.Key}{Guid.NewGuid():N}";

                string baseId = op.OperationId;
                string unique = baseId;
                int i = 1;
                while (!seen.Add(unique))
                    unique = $"{baseId}_{i++}";

                op.OperationId = unique;
            }
    }

    /// <summary>
    /// Rewrite <c>$ref</c> IDs everywhere in the document according to
    /// <paramref name="mapping"/> (old → new).  Walks the whole tree so
    /// references buried in <c>schema.Properties</c>, <c>items</c>, etc.
    /// can’t be missed.
    /// </summary>
    private static void UpdateAllReferences(OpenApiDocument doc, Dictionary<string, string> mapping)
    {
        /* ------------------------------------------------------------ */
        void RewriteRef(OpenApiReference? r)
        /* ------------------------------------------------------------ */
        {
            if (r == null) return;
            if (r.Type != ReferenceType.Schema) return; // only rewrite schema refs
            if (string.IsNullOrEmpty(r.Id)) return;

            if (mapping.TryGetValue(r.Id, out var newId))
                r.Id = newId;
        }

        /* ------------------------------------------------------------ */
        var visited = new HashSet<OpenApiSchema>();

        void WalkSchema(OpenApiSchema? s)
        /* ------------------------------------------------------------ */
        {
            if (s == null || !visited.Add(s)) return;

            RewriteRef(s.Reference);

            if (s.Properties != null)
                foreach (var child in s.Properties.Values)
                    WalkSchema(child);

            if (s.Items != null) WalkSchema(s.Items);

            foreach (var a in s.AllOf ?? Enumerable.Empty<OpenApiSchema>()) WalkSchema(a);
            foreach (var o in s.OneOf ?? Enumerable.Empty<OpenApiSchema>()) WalkSchema(o);
            foreach (var a in s.AnyOf ?? Enumerable.Empty<OpenApiSchema>()) WalkSchema(a);

            if (s.AdditionalProperties != null)
                WalkSchema(s.AdditionalProperties);
        }

        /* === 1. Components =================================================== */

        if (doc.Components?.Schemas != null)
            foreach (var schema in doc.Components.Schemas.Values)
                WalkSchema(schema);

        if (doc.Components?.Parameters != null)
            foreach (var p in doc.Components.Parameters.Values)
            {
                RewriteRef(p.Reference);
                WalkSchema(p.Schema);
            }

        if (doc.Components?.Headers != null)
            foreach (var h in doc.Components.Headers.Values)
            {
                RewriteRef(h.Reference);
                WalkSchema(h.Schema);
            }

        if (doc.Components?.RequestBodies != null)
            foreach (var rb in doc.Components.RequestBodies.Values)
            {
                RewriteRef(rb.Reference);
                foreach (var mt in rb.Content.Values)
                    WalkSchema(mt.Schema);
            }

        if (doc.Components?.Responses != null)
            foreach (var resp in doc.Components.Responses.Values)
            {
                RewriteRef(resp.Reference);
                foreach (var mt in resp.Content.Values)
                    WalkSchema(mt.Schema);
            }

        /* === 2. Paths/Operations ============================================= */

        foreach (var path in doc.Paths.Values)
            foreach (var op in path.Operations.Values)
            {
                /* request body */
                RewriteRef(op.RequestBody?.Reference);
                if (op.RequestBody?.Content != null)
                    foreach (var mt in op.RequestBody.Content.Values)
                        WalkSchema(mt.Schema);

                /* parameters */
                if (op.Parameters != null)
                    foreach (var p in op.Parameters)
                    {
                        RewriteRef(p.Reference);
                        WalkSchema(p.Schema);
                    }

                /* responses */
                foreach (var resp in op.Responses.Values)
                {
                    RewriteRef(resp.Reference);
                    foreach (var mt in resp.Content.Values)
                        WalkSchema(mt.Schema);
                }
            }
    }

    private static readonly Regex NonIdChars = new(@"[^A-Za-z0-9_]", RegexOptions.Compiled);
    private static string SanitizeName(string input)
    {
        if (string.IsNullOrWhiteSpace(input)) return "X";

        var cleaned = NonIdChars.Replace(input, "_");  // keep `_` so Kiota can split later
        cleaned = Regex.Replace(cleaned, @"_+", "_");  // collapse ___ → _
        cleaned = cleaned.Trim('_', '.');              // kill leading / trailing empties

        return string.IsNullOrWhiteSpace(cleaned) ? "X" : cleaned;
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

            // Handle duplicate account_id parameter
            if (originalPath.Contains("/accounts/{account_id}/addressing/address_maps/{address_map_id}/accounts/{account_id}"))
            {
                newPath = originalPath.Replace("/accounts/{account_id}/addressing/address_maps/{address_map_id}/accounts/{account_id}",
                    "/accounts/{account_id}/addressing/address_maps/{address_map_id}/accounts/{member_account_id}");

                // Update the parameter name in the operations
                foreach (var operation in kvp.Value.Operations.Values)
                {
                    if (operation.Parameters == null)
                    {
                        operation.Parameters = new List<OpenApiParameter>();
                    }

                    // Ensure both account_id parameters are present
                    var hasAccountId = operation.Parameters.Any(p => p.Name == "account_id" && p.In == ParameterLocation.Path);
                    var hasMemberAccountId = operation.Parameters.Any(p => p.Name == "member_account_id" && p.In == ParameterLocation.Path);

                    if (!hasAccountId)
                    {
                        operation.Parameters.Add(new OpenApiParameter
                        {
                            Name = "account_id",
                            In = ParameterLocation.Path,
                            Required = true,
                            Schema = new OpenApiSchema
                            {
                                Type = "string",
                                MaxLength = 32,
                                Description = "Identifier of a Cloudflare account."
                            }
                        });
                    }

                    if (!hasMemberAccountId)
                    {
                        operation.Parameters.Add(new OpenApiParameter
                        {
                            Name = "member_account_id",
                            In = ParameterLocation.Path,
                            Required = true,
                            Schema = new OpenApiSchema
                            {
                                Type = "string",
                                MaxLength = 32,
                                Description = "Identifier of the member account to add/remove from the Address Map."
                            }
                        });
                    }

                    // Update existing member_account_id parameter if it exists
                    foreach (var param in operation.Parameters)
                    {
                        if (param.Name == "member_account_id" && param.In == ParameterLocation.Path)
                        {
                            param.Schema ??= new OpenApiSchema();
                            param.Schema.Description = "Identifier of the member account to add/remove from the Address Map.";
                        }
                    }
                }
            }
            else if (originalPath.EndsWith("/item", StringComparison.OrdinalIgnoreCase))
            {
                newPath = originalPath.Replace("/item", "/item_static");
            }
            else if (originalPath.Contains("/item/{", StringComparison.OrdinalIgnoreCase))
            {
                newPath = originalPath.Replace("/item", "/item_by_id");
            }

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
                                         !s.AllOf.Any() // ← don't treat allOf children as "empty"
                                         && !s.AnyOf.Any() && !s.OneOf.Any());
        bool hasExample = s?.Example != null || (media.Examples?.Any() == true);
        return schemaEmpty && !hasExample;
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

    private void CleanEmptyKeysOn<T>(IDictionary<string, T> dict, string dictName)
    {
        if (dict == null) return;
        foreach (var key in dict.Keys.ToList())
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                _logger.LogWarning("Dropping empty key from " + dictName);
                dict.Remove(key);
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

        foreach (var path in document.Paths.Values)
        {
            foreach (var op in path.Operations.Values)
            {
                if (op.Parameters == null) continue;

                var rogue = op.Parameters.FirstOrDefault(p =>
                    p.In == ParameterLocation.Header && p.Name.StartsWith("authorization", StringComparison.OrdinalIgnoreCase));

                if (rogue != null)
                {
                    op.Parameters.Remove(rogue);

                    op.Security ??= new List<OpenApiSecurityRequirement>();
                    op.Security.Add(new OpenApiSecurityRequirement
                    {
                        [schemes["assets_jwt"]] = new List<string>()
                    });
                }
            }
        }
    }

    //private void FixInvalidDefaults(OpenApiDocument document)
    //{
    //    if (document.Components?.Schemas == null) return;

    //    var visited = new HashSet<OpenApiSchema>();
    //    foreach (var schema in document.Components.Schemas.Values)
    //    {
    //        FixSchemaDefaults(schema);
    //    }
    //}

    private static string CanonicalSuccess(OperationType op) => op switch
    {
        OperationType.Post => "201",
        OperationType.Delete => "204",
        _ => "200"
    };

    private void FixSchemaDefaults(OpenApiDocument doc)
    {
        var visited = new HashSet<OpenApiSchema>();

        void Visit(OpenApiSchema? s)
        {
            if (s == null || !visited.Add(s)) return;

            /* ---------- 1. Fix enum/default mismatches ---------- */
            if (s.Enum?.Any() == true && s.Default is OpenApiString defStr)
            {
                // Find case-insensitive match in the enum list
                var match = s.Enum
                             .OfType<OpenApiString>()
                             .FirstOrDefault(e => e.Value.Equals(defStr.Value,
                                                    StringComparison.OrdinalIgnoreCase));

                if (match != null)
                    s.Default = new OpenApiString(match.Value);   // correct case
                else
                    s.Default = null;                             // or drop it
            }

            /* ---------- 2. Recurse ---------- */
            if (s.Properties != null)
                foreach (var p in s.Properties.Values) Visit(p);
            Visit(s.Items);
            Visit(s.AdditionalProperties);

            foreach (var x in s.AllOf ?? Array.Empty<OpenApiSchema>()) Visit(x);
            foreach (var x in s.OneOf ?? Array.Empty<OpenApiSchema>()) Visit(x);
            foreach (var x in s.AnyOf ?? Array.Empty<OpenApiSchema>()) Visit(x);
        }

        //  A) every component schema
        foreach (var c in doc.Components?.Schemas?.Values ?? Enumerable.Empty<OpenApiSchema>())
            Visit(c);

        //  B) inline schemas hiding under parameters / headers / media-types
        foreach (var path in doc.Paths.Values)
            foreach (var op in path.Operations.Values)
            {
                foreach (var p in op.Parameters ?? Enumerable.Empty<OpenApiParameter>())
                    Visit(p.Schema);

                foreach (var mt in op.RequestBody?.Content?.Values ?? Enumerable.Empty<OpenApiMediaType>())
                    Visit(mt.Schema);

                foreach (var resp in op.Responses.Values)
                    foreach (var mt in resp.Content?.Values ?? Enumerable.Empty<OpenApiMediaType>())
                        Visit(mt.Schema);
            }

        //  C) component-level parameters / headers / bodies / responses
        void VisitMediaSchemas<T>(IDictionary<string, T>? dict) where T : class, IOpenApiReferenceable
        {
            if (dict == null) return;
            foreach (var v in dict.Values)
            {
                switch (v)
                {
                    case OpenApiParameter p: Visit(p.Schema); break;
                    case OpenApiHeader h: Visit(h.Schema); break;
                    case OpenApiRequestBody rb:
                        foreach (var mt in rb.Content.Values) Visit(mt.Schema); break;
                    case OpenApiResponse r:
                        foreach (var mt in r.Content.Values) Visit(mt.Schema); break;
                }
            }
        }

        var comps = doc.Components;
        VisitMediaSchemas(comps?.Parameters);
        VisitMediaSchemas(comps?.Headers);
        VisitMediaSchemas(comps?.RequestBodies);
        VisitMediaSchemas(comps?.Responses);
    }

    private void RemoveEmptyCompositionObjects(OpenApiSchema schema, HashSet<OpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema)) return;

        // Process nested schemas first
        if (schema.Properties != null)
        {
            schema.Properties = schema.Properties.GroupBy(p => p.Key).ToDictionary(g => g.Key, g => g.First().Value);

            foreach (var prop in schema.Properties.Values)
            {
                RemoveEmptyCompositionObjects(prop, visited);
            }
        }

        if (schema.Items != null)
        {
            RemoveEmptyCompositionObjects(schema.Items, visited);
        }

        if (schema.AdditionalProperties != null)
        {
            RemoveEmptyCompositionObjects(schema.AdditionalProperties, visited);
        }

        // Process allOf array
        if (schema.AllOf != null)
        {
            var validSchemas = schema.AllOf.Where(s => s != null && !IsEmptySchema(s)).ToList();

            if (!validSchemas.Any())
            {
                schema.AllOf = null;
            }
            else if (validSchemas.Count == 1)
            {
                // If only one schema remains, merge its properties into the parent
                var remaining = validSchemas[0];

                if (remaining.Reference == null)
                {
                    // merge & drop only when there is real inline content
                    if (remaining.Properties != null)
                    {
                        schema.Properties ??= new Dictionary<string, OpenApiSchema>();
                        foreach (var p in remaining.Properties)
                            schema.Properties[p.Key] = p.Value;
                    }

                    if (remaining.Required != null)
                    {
                        schema.Required ??= new HashSet<string>();
                        foreach (var req in remaining.Required)
                            schema.Required.Add(req);
                    }

                    schema.AllOf = null; // remove wrapper
                }
            }
        }

        // Process oneOf array
        if (schema.OneOf != null)
        {
            var validSchemas = schema.OneOf.Where(s => s != null && !IsEmptySchema(s)).ToList();

            if (!validSchemas.Any())
            {
                schema.OneOf = null;
            }
            else if (validSchemas.Count == 1)
            {
                // If only one schema remains, merge its properties into the parent
                var remaining = validSchemas[0];
                if (remaining.Reference == null)
                {
                    // merge & drop only when there is real inline content
                    if (remaining.Properties != null)
                    {
                        schema.Properties ??= new Dictionary<string, OpenApiSchema>();
                        foreach (var p in remaining.Properties)
                            schema.Properties[p.Key] = p.Value;
                    }

                    if (remaining.Required != null)
                    {
                        schema.Required ??= new HashSet<string>();
                        foreach (var req in remaining.Required)
                            schema.Required.Add(req);
                    }

                    schema.AllOf = null; // remove wrapper
                }
            }
        }

        // Process anyOf array
        if (schema.AnyOf != null)
        {
            var validSchemas = schema.AnyOf.Where(s => s != null && !IsEmptySchema(s)).ToList();

            if (!validSchemas.Any())
            {
                schema.AnyOf = null;
            }
            else if (validSchemas.Count == 1)
            {
                // If only one schema remains, merge its properties into the parent
                var remaining = validSchemas[0];
                if (remaining.Reference == null)
                {
                    // merge & drop only when there is real inline content
                    if (remaining.Properties != null)
                    {
                        schema.Properties ??= new Dictionary<string, OpenApiSchema>();
                        foreach (var p in remaining.Properties)
                            schema.Properties[p.Key] = p.Value;
                    }

                    if (remaining.Required != null)
                    {
                        schema.Required ??= new HashSet<string>();
                        foreach (var req in remaining.Required)
                            schema.Required.Add(req);
                    }

                    schema.AllOf = null; // remove wrapper
                }

                schema.AnyOf = null;
            }
        }
    }

    private static void SanitizeExample(OpenApiSchema s)
    {
        if (s?.Example is OpenApiArray arr && arr.Any())
        {
            // Keep the first primitive if it matches the schema type; otherwise nuke it
            if (s.Type == "string" && arr.First() is OpenApiString os)
                s.Example = new OpenApiString(os.Value);
            else
                s.Example = null;
        }

        // Drop multi-KB blobs (e.g., huge JWT samples)
        if (s?.Example is OpenApiString str && str.Value?.Length > 5_000)
            s.Example = null;
    }

    private bool IsEmptySchema(OpenApiSchema schema)
    {
        if (schema == null) return true;

        return schema.Reference == null &&(schema.Properties == null || !schema.Properties.Any()) &&
               (schema.AllOf == null || !schema.AllOf.Any()) && (schema.OneOf == null || !schema.OneOf.Any()) &&
               (schema.AnyOf == null || !schema.AnyOf.Any()) && schema.Items == null && (schema.Enum == null || !schema.Enum.Any()) &&
               schema.AdditionalProperties == null && !schema.AdditionalPropertiesAllowed;
    }


    private void EnsureTitlesOnCompositionBranches(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null) return;
        var visited = new HashSet<OpenApiSchema>();

        foreach (var (compKey, rootSchema) in doc.Components.Schemas)
        {
            var rootLabel = !string.IsNullOrWhiteSpace(rootSchema.Title)
                ? rootSchema.Title
                : compKey;

            Walk(rootSchema, rootLabel);
        }

        /* ------------------------------------------------ */
        void Walk(OpenApiSchema s, string parentName)
            /* ------------------------------------------------ */
        {
            if (s == null || !visited.Add(s)) return;

            if (s.Reference == null && string.IsNullOrWhiteSpace(s.Title))
                s.Title = $"{parentName}_Inline";

            // pass parentName along so NameList can use it
            NameList(s.AllOf, "AllOf", parentName);
            NameList(s.AnyOf, "AnyOf", parentName);
            NameList(s.OneOf, "OneOf", parentName);

            if (s.Properties != null)
                foreach (var p in s.Properties.Values)
                    Walk(p, parentName);

            if (s.Items != null)
                Walk(s.Items, parentName);

            if (s.AdditionalProperties != null)
                Walk(s.AdditionalProperties, parentName);
        }

        /* ------------------------------------------------ */
        void NameList(IList<OpenApiSchema>? list, string tag, string parentName)
            /* ------------------------------------------------ */
        {
            if (list == null) return;

            for (int i = 0; i < list.Count; i++)
            {
                var child = list[i];
                if (child?.Reference == null && string.IsNullOrWhiteSpace(child.Title))
                    child.Title = $"{parentName}_{tag}_{i + 1}";

                // recurse with the (now guaranteed) child title
                Walk(child, child.Title ?? parentName);
            }
        }
    }

    private void EnsureUniqueSchemaTitles(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null) return;

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (key, schema) in doc.Components.Schemas.ToList())
        {
            string wanted = string.IsNullOrWhiteSpace(schema.Title) ? key : schema.Title!;
            string unique = wanted;
            int i = 2;
            while (!seen.Add(unique))
                unique = $"{wanted}_{i++}";

            schema.Title = unique;                       // enforce uniqueness
        }
    }

    private void EnsureObjectTypeOnInlineSchemas(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null) return;

        var visited = new HashSet<OpenApiSchema>();
        void Walk(OpenApiSchema s)
        {
            if (s == null || !visited.Add(s)) return;

            // --------- < THE FIX > ----------
            if (s.Reference == null
                && string.IsNullOrWhiteSpace(s.Type)
                && (s.Properties?.Any() == true || s.Required?.Any() == true))
            {
                s.Type = "object";
            }
            // --------------------------------

            if (s.Properties != null) foreach (var p in s.Properties.Values) Walk(p);
            Walk(s.Items); Walk(s.AdditionalProperties);
            foreach (var b in s.AllOf ?? Enumerable.Empty<OpenApiSchema>()) Walk(b);
            foreach (var b in s.AnyOf ?? Enumerable.Empty<OpenApiSchema>()) Walk(b);
            foreach (var b in s.OneOf ?? Enumerable.Empty<OpenApiSchema>()) Walk(b);
        }

        foreach (var root in doc.Components.Schemas.Values) Walk(root);
    }

    void HydrateParameterNames(OpenApiDocument doc)
    {
        if (doc.Components?.Parameters == null) return;

        foreach (var pathItem in doc.Paths.Values)
        foreach (var op in pathItem.Operations.Values)
        foreach (var p in op.Parameters?.ToList() ?? new List<OpenApiParameter>())
        {
            if (!string.IsNullOrWhiteSpace(p.Name)) continue;          // already good
            if (p.Reference?.Id == null) continue;                     // inline – we already dropped empties

            if (doc.Components.Parameters.TryGetValue(p.Reference.Id, out var target))
            {
                p.Name = target.Name;
                p.In = target.In;
                p.Required |= target.Required;
            }
            else
            {
                _logger.LogWarning("Parameter $ref '{Id}' has no matching definition – dropping", p.Reference.Id);
                op.Parameters.Remove(p);                               // keeps spec valid
            }
        }
    }

    private void InjectTypeForNullable(OpenApiSchema schema, HashSet<OpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema))
            return;

        if (schema.Nullable && string.IsNullOrWhiteSpace(schema.Type))
        {
            schema.Type = "object";
            _logger.LogWarning("Injected default type='object' for nullable schema '{SchemaTitle}'", schema.Title ?? "(no title)");
        }

        // recurse
        if (schema.Properties != null)
            foreach (var prop in schema.Properties.Values)
                InjectTypeForNullable(prop, visited);

        if (schema.Items != null)
            InjectTypeForNullable(schema.Items, visited);

        if (schema.AdditionalProperties != null)
            InjectTypeForNullable(schema.AdditionalProperties, visited);

        if (schema.AllOf != null)
            foreach (var s in schema.AllOf)
                InjectTypeForNullable(s, visited);

        if (schema.OneOf != null)
            foreach (var s in schema.OneOf)
                InjectTypeForNullable(s, visited);

        if (schema.AnyOf != null)
            foreach (var s in schema.AnyOf)
                InjectTypeForNullable(s, visited);
    }
}