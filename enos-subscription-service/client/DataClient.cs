using enos_subscription.proto;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace enos_subscription.client
{
    public class DataClient : SubClient
    {
        public DataClient(string _host, int _port, string _accessKey, string _accessSecret) : base(_host, _port, _accessKey, _accessSecret) { }

        public override int subType { get { return 0; } }
    }
}
