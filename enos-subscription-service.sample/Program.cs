using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using enos_subscription_service.client;

namespace enos_subscription_service.sample
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine(string.Format("connecting to: ws://{0}:{1}", ConfigurationManager.AppSettings["host"], ConfigurationManager.AppSettings["port"]));


            using (DataClient client = new DataClient(ConfigurationManager.AppSettings["host"], int.Parse(ConfigurationManager.AppSettings["port"]), ConfigurationManager.AppSettings["accesskey"], ConfigurationManager.AppSettings["accesssecret"]))
            {
                try
                {
                    client.subscribe(ConfigurationManager.AppSettings["subid"]);
                    foreach (var message in client.GetMessages())
                    {
                        Console.WriteLine("got message on client: " + message.key);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }

            Console.ReadLine();
        }
    }
}
