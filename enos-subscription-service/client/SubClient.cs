using enos_subscription_service.core;
using enos_subscription_service.proto;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace enos_subscription_service.client
{
    public abstract class SubClient : IDisposable
    {
        private string host { get; set; }
        private int port { get; set; }
        private string accessKey { get; set; }
        private string accessSecret { get; set; }
        //private string sub_id { get; set; }
        public abstract int subType { get; }

        private BaseClient client;
        private int auto_commit_interval_seconds = 5;
        private DateTime next_auto_commit_deadline { get; set; }
        private bool is_started = false;
        private List<Message> consumer_offset = new List<Message>();

        private static NLog.Logger _logger = NLog.LogManager.GetCurrentClassLogger();
        
        public SubClient(string _host, int _port, string _accessKey, string _accessSecret)
        {
            host = _host;
            port = _port;
            accessKey = _accessKey;
            accessSecret = _accessSecret;
        }
        public void subscribe(string _sub_id, string _consumer_group = "DefaultConsumerGroup")
        {
            try
            {
                next_auto_commit_deadline = DateTime.Now.AddSeconds(auto_commit_interval_seconds);
                client = new BaseClient(host, port, accessKey, accessSecret, _sub_id, subType, _consumer_group);
                client.start();
                is_started = true;
                _logger.Info("Subscribe successful", _sub_id);
            }
            catch(Exception ex)
            {
                _logger.Error(ex, "Subscribe failed", _sub_id);
                is_started = false;
                throw;
            }
        }

        private void do_commit()
        {
            try
            {
                CommitDTO commit_dto = new CommitDTO();
                foreach (Message message in consumer_offset)
                {
                    commit_dto.commits.Add(new Commit
                    {
                        topic = message.topic,
                        partition = message.partition,
                        offset = message.offset
                    });
                }
                client.commit_offsets(commit_dto);
            }
            catch
            {
                //_logger.log
            }
        }

        public IEnumerable<Message> GetMessages()
        {
            while (is_started)
            {
                if (DateTime.Now > next_auto_commit_deadline && consumer_offset.Count > 0)
                {
                    do_commit();
                    consumer_offset.Clear();
                    next_auto_commit_deadline = next_auto_commit_deadline.AddSeconds(auto_commit_interval_seconds);
                }
                //Console.WriteLine("requesting for message at:" + DateTime.Now.ToString("HH:mm:ss"));
                var message = client.poll();
                consumer_offset.Add(message);
                _logger.Info(string.Format("Polled message, key: {0}, partition: {1}, offset: {2}" , message.key, message.partition, message.offset));
                yield return message;
            }
        }

        public void Dispose()
        {
            client.Dispose();
        }
    }
}
