using enos_subscription.core;
using enos_subscription.proto;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace enos_subscription.client
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


        private Dictionary<string, Message> consumer_offset = new Dictionary<string, Message>();

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
                client = new BaseClient(host, port, accessKey, accessSecret, _sub_id, subType, _consumer_group);
                client.start();
                is_started = true;
                next_auto_commit_deadline = DateTime.Now.AddSeconds(auto_commit_interval_seconds);
                _logger.Info("Subscribe successful", _sub_id);
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Subscribe failed", _sub_id);
                is_started = false;
                throw;
            }
        }

        private void do_commit()
        {
            if (consumer_offset.Count == 0)
                return;
            try
            {
                CommitDTO commit_dto = new CommitDTO();

                foreach (string key in consumer_offset.Keys)
                {
                    var message = consumer_offset[key];
                    if (message != null)
                    {
                        commit_dto.commits.Add(new Commit
                        {
                            topic = message.topic,
                            partition = message.partition,
                            offset = message.offset
                        });
                    }
                }
                client.commit_offsets(commit_dto);
            }
            catch
            {
                //_logger.log
            }
        }

        private void addToOffset(Message message)
        {
            string key = string.Format("{0}_{1}", message.topic, message.partition);
            if (consumer_offset.ContainsKey(key))
            {
                consumer_offset[key] = message;
            }
            else
                consumer_offset.Add(key, message);
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
                var message = client.poll();
                addToOffset(message);

                if (message.value != "_*NMM!_")
                {
                    _logger.Trace(string.Format("Polled message, key: {0}, partition: {1}, offset: {2}", message.key, message.partition, message.offset));
                    yield return message;
                }
                else
                {
                    _logger.Trace(string.Format("Polled _*NMM!_ message, key: {0}, partition: {1}, offset: {2}", message.key, message.partition, message.offset));
                    break;
                }
            }
        }

        public void Dispose()
        {
            client.Dispose();
        }
    }
}
