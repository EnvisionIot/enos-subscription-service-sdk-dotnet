using enos_subscription.proto;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace enos_subscription.util
{
    public class ProtoBufDecoder
    {
        internal static T DeserializeToObj<T>(byte[] message, bool needPrefix = false)
        {
            using (MemoryStream stream = new MemoryStream(message))
            {
                if (needPrefix)
                    return Serializer.DeserializeWithLengthPrefix<T>(stream, PrefixStyle.Base128);
                else
                    return Serializer.Deserialize<T>(stream);
            }
        }
    }
}
