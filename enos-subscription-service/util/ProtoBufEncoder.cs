using enos_subscription_service.proto;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace enos_subscription_service.util
{
    public class ProtoBufEncoder
    {
        internal static byte[] SerializeToBytes<T>(T instance, bool needPrefix = false)
        {
            byte[] bytes;
            using (MemoryStream stream = new MemoryStream())
            {
                stream.SetLength(0);
                if (needPrefix)
                { 
                    Serializer.SerializeWithLengthPrefix(stream, instance, PrefixStyle.Base128);
                }
                else
                {
                    Serializer.Serialize(stream, instance);
                }
                bytes = stream.ToArray();
            }
            return bytes;
        }
        /*
        internal static ByteString SerializeAuthReqToByteString(AuthReq instance)
        {
           return instance.ToByteString();
           
            using (MemoryStream stream = new MemoryStream())
            {
                stream.SetLength(0);
                instance.WriteTo(stream);
                return ByteString.CopyFrom(stream.ToArray());
            }
            
        }
        internal static byte[] SerializeTransferPkgToBytes(TransferPkg instance)
        {  
            using (MemoryStream stream = new MemoryStream())
            {
                stream.SetLength(0);
                instance.WriteTo(stream);
                return stream.ToArray();
            }
        }
    */
    }
}
