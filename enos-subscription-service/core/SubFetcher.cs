using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace enos_subscription.core
{
    public class SubFetcher
    {
        //private BaseClient client;
        public void Run(BaseClient _client)
        {
            int epoch = _client.epoch;
            //Thread.Sleep(2000);
            while (_client.isConnected)
            {
                if (epoch != _client.epoch)
                {
                    return;
                }
                _client.pull_once();

            }
        }
    }
}
