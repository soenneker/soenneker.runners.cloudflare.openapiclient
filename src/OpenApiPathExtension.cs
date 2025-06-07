using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Soenneker.Runners.Cloudflare.OpenApiClient
{
    public static class OpenApiPathExtension
    {
        // replace only the first occurrence of `search` in `text`
        public static string ReplaceFirst(this string text, string search, string replace)
        {
            var idx = text.IndexOf(search, StringComparison.Ordinal);
            if (idx < 0) return text;
            return text.Substring(0, idx)
                   + replace
                   + text.Substring(idx + search.Length);
        }
    }
}
