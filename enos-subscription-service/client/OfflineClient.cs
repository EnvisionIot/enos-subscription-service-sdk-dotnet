using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace enos_subscription_service.client
{
    public class OfflineClient : SubClient
    {
        public OfflineClient(string _host, int _port, string _accessKey, string _accessSecret) : base(_host, _port, _accessKey, _accessSecret) { }

        public override int subType { get { return 3; } }
    }
}
