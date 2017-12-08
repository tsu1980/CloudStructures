using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudStructures.Jil
{
    public class JilRedisValueConverter : ObjectRedisValueConverterBase
    {
        protected override object DeserializeCore(Type type, byte[] value)
        {
            using (var ms = new MemoryStream(value))
            using (var sr = new StreamReader(ms, Encoding.UTF8))
            {
                var result = global::Jil.JSON.Deserialize(sr, type);
                return result;
            }
        }

        protected override byte[] SerializeCore(object value, out long resultSize)
        {
            using (var ms = new MemoryStream())
            {
                using (var sw = new StreamWriter(ms, Encoding.UTF8))
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
