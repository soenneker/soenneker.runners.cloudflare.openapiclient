using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using System.Collections.Generic;
using System.Linq;

namespace Soenneker.Runners.Cloudflare.OpenApiClient.Utils;

public static class OpenApiSchemaUtilities
{
    /// <summary>
    /// Strips every Discriminator (and its mappings) from all component schemas
    /// to prevent Kiota from attempting polymorphic inheritance on them.
    /// </summary>
    public static void StripAllDiscriminators(OpenApiDocument document, ILogger logger)
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

    public static string NormalizeMediaType(string mediaType)
    {
        if (string.IsNullOrWhiteSpace(mediaType))
            return "application/json";
        string baseType = mediaType.Split(';')[0].Trim();
        if (baseType.Contains('*') || !baseType.Contains('/'))
            return "application/json";
        return baseType;
    }

    public static bool IsMediaEmpty(OpenApiMediaType media)
    {
        OpenApiSchema? s = media.Schema;
        bool schemaEmpty = s == null || (string.IsNullOrWhiteSpace(s.Type) && (s.Properties == null || !s.Properties.Any()) && s.Items == null &&
                                         !s.AllOf.Any() // ← don't treat allOf children as "empty"
                                         && !s.AnyOf.Any() && !s.OneOf.Any());
        bool hasExample = s?.Example != null || (media.Examples?.Any() == true);
        return schemaEmpty && !hasExample;
    }

    public static bool IsSchemaEmpty(OpenApiSchema schema)
    {
        return schema == null || (string.IsNullOrWhiteSpace(schema.Type) && (schema.Properties == null || !schema.Properties.Any()) && !schema.AllOf.Any() &&
                                  !schema.OneOf.Any() && !schema.AnyOf.Any() && schema.Items == null && (schema.Enum == null || !schema.Enum.Any()) &&
                                  schema.AdditionalProperties == null && !schema.AdditionalPropertiesAllowed);
    }

    public static void EnsureResponseDescriptions(OpenApiResponses responses)
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

    public static void CleanEmptyKeysOn<T>(IDictionary<string, T> dict, string dictName, ILogger logger)
    {
        if (dict == null) return;
        foreach (var key in dict.Keys.ToList())
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                logger.LogWarning("Dropping empty key from " + dictName);
                dict.Remove(key);
            }
        }
    }
} 