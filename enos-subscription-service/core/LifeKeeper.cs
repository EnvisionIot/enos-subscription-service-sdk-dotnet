using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace enos_subscription_service.core
{
    public class LifeKeeper
    {
        //private BaseClient client;
        public void Run(BaseClient _client)
        {
            int epoch = _client.epoch;
            while (_client.isConnected)
            {
                if (epoch != _client.epoch)
                {
                    return;
                }
                if (DateTime.Now > _client.next_ping_deadline)
                {
                    _client.ping_and_recv();
                }
                else
                {
                    Thread.Sleep(1000);
                }
            }
        }
    }
}
