using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace enos_subscription.util
{
  public  class Encryptor
    {
        internal static string GetHashSha256(string _access_key, string _sub_id, string _secret)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("access_key");
            sb.Append(_access_key);
            sb.Append("access_secret");
            sb.Append(_secret);
            sb.Append("sub_id");
            sb.Append(_sub_id);
            byte[] bytes = Encoding.UTF8.GetBytes(sb.ToString());
            SHA256Managed hashstring = new SHA256Managed();
            byte[] hash = hashstring.ComputeHash(bytes);
            string hashString = string.Empty;

            foreach (byte x in hash)
            {
                hashString += String.Format("{0:x2}", x);
            }
            return hashString;
        }
    }
}
