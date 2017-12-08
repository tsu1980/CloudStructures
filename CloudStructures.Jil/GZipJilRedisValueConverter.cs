using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudStructures.Jil
{
    public class GZipJilRedisValueConverter : ObjectRedisValueConverterBase
    {
        readonly System.IO.Compression.CompressionLevel compressionLevel;

        public GZipJilRedisValueConverter()
            : this(System.IO.Compression.CompressionLevel.Fastest)
        {

        }

        public GZipJilRedisValueConverter(System.IO.Compression.CompressionLevel compressionLevel)
        {
            this.compressionLevel = compressionLevel;
        }

        protected override object DeserializeCore(Type type, byte[] value)
        {
            using (var ms = new MemoryStream(value))
            using (var gzip = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionMode.Decompress))
            using (var sr = new StreamReader(gzip, Encoding.UTF8))
            {
                var result = global::Jil.JSON.Deserialize(sr, type);
                return result;
            }
        }

        protected override byte[] SerializeCore(object value, out long resultSize)
        {
            using (var ms = new MemoryStream())
            {
                using (var gzip = new System.IO.Compression.GZipStream(ms, compressionLevel))
                using (var sw = new StreamWriter(gzip))
                {
                    global::Jil.JSON.Serialize(value, sw);
                }
                var result = ms.ToArray();
                resultSize = result.Length;
                return result;
            }
        }
    }
}
