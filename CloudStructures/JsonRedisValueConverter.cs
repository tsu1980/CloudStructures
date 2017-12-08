using Newtonsoft.Json;
using System;
using System.IO;
using System.Text;

namespace CloudStructures
{
    public class JsonRedisValueConverter : ObjectRedisValueConverterBase
    {
        static readonly Encoding encoding = new UTF8Encoding(false);
        readonly JsonSerializer serializer;

        public JsonRedisValueConverter()
            : this(new JsonSerializer { Formatting = Formatting.None })
        {

        }

        public JsonRedisValueConverter(JsonSerializer serializer)
        {
            this.serializer = serializer;
        }

        protected override object DeserializeCore(Type type, byte[] value)
        {
            using (var ms = new MemoryStream(value))
            using (var sr = new StreamReader(ms, encoding))
            {
                var result = serializer.Deserialize(sr, type);
                return result;
            }
        }

        protected override byte[] SerializeCore(object value, out long resultSize)
        {
            using (var ms = new MemoryStream())
            {
                using (var sw = new StreamWriter(ms, encoding))
                {
                    serializer.Serialize(sw, value);
                }
                var result = ms.ToArray();
                resultSize = result.Length;
                return result;
            }
        }
    }

    public class GZipJsonRedisValueConverter : ObjectRedisValueConverterBase
    {
        static readonly Encoding encoding = new UTF8Encoding(false);
        readonly JsonSerializer serializer;
        readonly System.IO.Compression.CompressionLevel compressionLevel;

        public GZipJsonRedisValueConverter()
            : this(System.IO.Compression.CompressionLevel.Fastest)
        {

        }

        public GZipJsonRedisValueConverter(System.IO.Compression.CompressionLevel compressionLevel)
            : this(compressionLevel, new JsonSerializer { Formatting = Formatting.None })
        {
        }

        public GZipJsonRedisValueConverter(System.IO.Compression.CompressionLevel compressionLevel, JsonSerializer serializer)
        {
            this.compressionLevel = compressionLevel;
            this.serializer = serializer;
        }

        protected override object DeserializeCore(Type type, byte[] value)
        {
            using (var ms = new MemoryStream(value))
            using (var gzip = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionMode.Decompress))
            using (var sr = new StreamReader(gzip, encoding))
            {
                var result = serializer.Deserialize(sr, type);
                return result;
            }
        }

        protected override byte[] SerializeCore(object value, out long resultSize)
        {
            using (var ms = new MemoryStream())
            {
                using (var gzip = new System.IO.Compression.GZipStream(ms, compressionLevel))
                using (var sw = new StreamWriter(gzip, encoding))
                {
                    serializer.Serialize(sw, value);
                }
                var result = ms.ToArray();
                resultSize = result.Length;
                return result;
            }
        }
    }
}