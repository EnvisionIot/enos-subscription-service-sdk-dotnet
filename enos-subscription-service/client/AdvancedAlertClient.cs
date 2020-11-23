using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace enos_subscription.client
{
    public class AdvancedAlertClient : SubClient
    {
        public AdvancedAlertClient(string _host, int _port, string _accessKey, string _accessSecret) : base(_host, _port, _accessKey, _accessSecret) { }

        public override int subType { get { return 5; } }
    }
}
