using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using enos_subscription.client;

namespace enos_subscription_service.sample
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine(string.Format("connecting to: ws://{0}:{1}", ConfigurationManager.AppSettings["host"], ConfigurationManager.AppSettings["port"]));

            switch (ConfigurationManager.AppSettings["client_type"])
            {
                case "0":
                    using (DataClient client = new DataClient(ConfigurationManager.AppSettings["host"], int.Parse(ConfigurationManager.AppSettings["port"]), ConfigurationManager.AppSettings["accesskey"], ConfigurationManager.AppSettings["accesssecret"]))
                    {
                        try
                        {
                            client.subscribe(ConfigurationManager.AppSettings["subid"], ConfigurationManager.AppSettings["consumergroup"]);
                            foreach (var message in client.GetMessages())
                            {
                                Console.WriteLine(string.Format("got message on client, key: {0}, partition: {1}, offset: {2}, value: {3}", message.key, message.partition, message.offset, message.value));
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                        }
                    }
                    break;
                case "1":
                    using (AlertClient client = new AlertClient(ConfigurationManager.AppSettings["host"], int.Parse(ConfigurationManager.AppSettings["port"]), ConfigurationManager.AppSettings["accesskey"], ConfigurationManager.AppSettings["accesssecret"]))
                    {
                        try
                        {
                            client.subscribe(ConfigurationManager.AppSettings["subid"], ConfigurationManager.AppSettings["consumergroup"]);
                            foreach (var message in client.GetMessages())
                            {
                                Console.WriteLine(string.Format("got message on client, key: {0}, partition: {1}, offset: {2}, value: {3}", message.key, message.partition, message.offset, message.value));
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                        }
                    }
                    break;
                case "3":
                    using (OfflineClient client = new OfflineClient(ConfigurationManager.AppSettings["host"], int.Parse(ConfigurationManager.AppSettings["port"]), ConfigurationManager.AppSettings["accesskey"], ConfigurationManager.AppSettings["accesssecret"]))
                    {
                        try
                        {
                            client.subscribe(ConfigurationManager.AppSettings["subid"], ConfigurationManager.AppSettings["consumergroup"]);
                            foreach (var message in client.GetMessages())
                            {
                                Console.WriteLine(string.Format("got message on client, key: {0}, partition: {1}, offset: {2}, value: {3}", message.key, message.partition, message.offset, message.value));
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                        }
                    }
                    break;
                default: break;
            }



            Console.ReadLine();
        }
    }
}
