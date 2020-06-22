using enos_subscription.proto;
using enos_subscription.util;
using ProtoBuf;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace enos_subscription.core
{
    public class BaseClient : IDisposable
    {
        private string host { get; set; }
        private int port { get; set; }
        private string accessKey { get; set; }
        private string accessSecret { get; set; }
        private string sub_id { get; set; }
        private int subType { get; set; }
        private string consumer_group { get; set; }
        public int pull_id { get; set; } = 0;
        public int epoch { get; set; } = 0;
        public DateTime next_ping_deadline { get; set; }
        private readonly object lockobj = new object();

        private readonly int ping_interval_in_sec = 10;
        private readonly int ping_timeout_in_millsec = 500;
        private readonly int DEFAULT_MESSAGE_QUEUE_SIZE = 100;

        private static NLog.Logger _logger = NLog.LogManager.GetCurrentClassLogger();
        private BlockingCollection<Message> queue = null;
        internal bool isConnected = false;
        internal TcpClient clientSocket = null;
        NetworkStream stream = null;// clientSocket.GetStream()

        public BaseClient(string _host, int _port, string _accessKey, string _accessSecret, string _sub_id, int _sub_type = 0, string _consumer_group = "")
        {
            host = _host;
            port = _port;
            accessKey = _accessKey;
            accessSecret = _accessSecret;
            sub_id = _sub_id;
            subType = _sub_type;
            consumer_group = _consumer_group;
        }

        public void start()
        {
            epoch++;
            queue = new BlockingCollection<Message>(DEFAULT_MESSAGE_QUEUE_SIZE);
            connect();
        }
        void reconnect()
        {
            _logger.Info("Reconnecting...");
            Dispose();
            start();
        }
        void connect()
        {
            while (clientSocket == null || !clientSocket.Connected)
            {
                try
                {
                    IPAddress ip;
                    if (!IPAddress.TryParse(host, out ip))
                    {
                        IPHostEntry ipHostInfo = Dns.GetHostEntry(host);
                        ip = ipHostInfo.AddressList[0];
                    }

                    IPEndPoint remoteEP = new IPEndPoint(ip, port);

                    clientSocket = new TcpClient(AddressFamily.InterNetwork);
                    clientSocket.Connect(remoteEP);
                    stream = clientSocket.GetStream();
                }
                catch (Exception ex)
                {
                    _logger.Error(ex, "connection failed, reconnecting...");
                    Thread.Sleep(1000);
                }
            }
            _logger.Info("connection established successfully.");
            auth();
        }
        void auth()
        {
            AuthReq auth_req = new AuthReq();

            auth_req.accessKey = accessKey;
            auth_req.subId = sub_id;
            auth_req.sign = Encryptor.GetHashSha256(accessKey, sub_id, accessSecret);
            auth_req.subType = subType;

            TransferPkg pkg = build_TransferPkg((int)CmdId.AuthReq, ProtoBufEncoder.SerializeToBytes(auth_req));

            TransferPkg auth_res = send_and_recv(pkg);

            if (auth_res == null)
            {
                _logger.Error("Authentication failed, received unexpected response, needed AuthRsp, received nothing.");
                reconnect();
                return;
            }
            //check if auth is successful
            if (auth_res.cmdId == (int)CmdId.AuthRsp)
            {
                AuthRsp rsp = ProtoBufDecoder.DeserializeToObj<AuthRsp>(auth_res.data);
                if (rsp.ack == 0)
                {
                    _logger.Info("Authenticated successfully.");
                    //subscribe
                    sub();
                }
                else
                {
                    throw new Exception("Authentication failed, auth info:" + auth_req.ToString());
                }
            }
            else
            {
                _logger.Error("Authentication failed, received unexpected response, needed AuthRsp, received:" + auth_res.cmdId);
            }
        }

        void sub()
        {
            SubReq sub_req = new SubReq();

            sub_req.category = subType;
            sub_req.clientId = ((IPEndPoint)(clientSocket.Client.LocalEndPoint)).Address.ToString();
            sub_req.subId = sub_id;
            sub_req.accessKey = accessKey;
            sub_req.consumerGroup = consumer_group;

            TransferPkg pkg = build_TransferPkg((int)CmdId.SubReq, ProtoBufEncoder.SerializeToBytes(sub_req));

            TransferPkg sub_res = send_and_recv(pkg);

            //check if subscription is successful
            if (sub_res == null)
            {
                _logger.Error("Subscription failed, receive unexpected response, needed SubRsp, received nothing.");
                reconnect();
                return;
            }
            if (sub_res.cmdId == (int)CmdId.SubRsp)
            {
                SubRsp rsp = ProtoBufDecoder.DeserializeToObj<SubRsp>(sub_res.data);
                if (rsp.ack == 0)
                {
                    isConnected = true;
                    //new thread to ping server to keep it alive
                    next_ping_deadline = DateTime.Now.AddSeconds(ping_interval_in_sec);
                    LifeKeeper lk = new LifeKeeper();
                    Thread life_keeper = new Thread(() => lk.Run(this));
                    life_keeper.Start();
                    //new thread to run fetching
                    SubFetcher fetcher = new SubFetcher();
                    Thread fetchthread = new Thread(() => fetcher.Run(this));
                    fetchthread.Start();
                }
                else
                {
                    _logger.Error("Subscription failed, sub info:" + sub_req.ToString());
                    reconnect();
                }
            }
            else
            {
                _logger.Error("Subscription failed, receive unexpected response, needed SubRsp, received:" + sub_res.cmdId);
                reconnect();
            }
        }

        public void pull_once()
        {
            if (!isConnected)
                return;

            PullReq pull_req = new PullReq();
            pull_id++;
            pull_req.id = pull_id;

            _logger.Trace("Pulling data: " + pull_id);

            TransferPkg pkg = build_TransferPkg((int)CmdId.PullReq, ProtoBufEncoder.SerializeToBytes(pull_req));

            TransferPkg pull_res = send_and_recv(pkg, true);
            if (pull_res == null)
            {
                _logger.Error("Pull failed, received unexpected response, needed PullRsp, received nothing.");
                reconnect();
                return;
            }
            if (pull_res.cmdId == (int)CmdId.PullRsp)
            {
                next_ping_deadline = DateTime.Now.AddSeconds(ping_interval_in_sec);
                PullRsp rsp = ProtoBufDecoder.DeserializeToObj<PullRsp>(pull_res.data);
                if (rsp.msgDTO.messages.Count > 0)
                {
                    _logger.Info("Got " + rsp.msgDTO.messages.Count + " message(s).");
                }
                foreach (var message in rsp.msgDTO.messages)
                {
                    _logger.Trace(string.Format("Got message, key: {0}, partition: {1}, offset: {2}, topic: {3}", message.key, message.partition, message.offset, message.topic));
                    queue.Add(message);
                }
            }
            else
            {
                _logger.Error("Pull failed, received unexpected response, needed PullRsp, received " + pull_res.cmdId);
            }

        }

        public void commit_offsets(CommitDTO commit_dto)
        {

            if (!isConnected || commit_dto == null || commit_dto.commits.Count == 0)
                return;

            _logger.Info("committing offset...");

            foreach (var commit in commit_dto.commits)
            {
                _logger.Trace(string.Format("committing topic: {0}, partition: {1}, offset: {2}  ", commit.topic, commit.partition, commit.offset));
            }

            TransferPkg pkg = build_TransferPkg((int)CmdId.CommitReq, ProtoBufEncoder.SerializeToBytes(commit_dto));

            TransferPkg commit_res = send_and_recv(pkg, false, false);
            if (commit_res == null)
            {
                _logger.Error("Commit failed, received unexpected response, needed CommitRsp, received nothing.");
                reconnect();
                return;
            }
            if (commit_res.cmdId != -3)
            {
                _logger.Error("Error occured commiting offset, reconnecting...");
            }
        }
        public Message poll()
        {
            _logger.Trace("polling message...");
            return queue.Take();
        }
        TransferPkg send_and_recv(TransferPkg pkg, bool is_pull_req = false, bool need_recv = true)
        {
            lock (lockobj)
            {
                try
                {
                    //write into stream
                    Serializer.SerializeWithLengthPrefix(stream, pkg, PrefixStyle.Base128);
                    if (need_recv)
                        //get response
                        return Serializer.DeserializeWithLengthPrefix<TransferPkg>(stream, PrefixStyle.Base128);
                    else
                    {
                        return new TransferPkg { cmdId = -3 };
                    }                
                }
                catch (Exception ex)
                {
                    _logger.Error("Error sending and receiving message: " + ex.Message);
                    reconnect();
                }

                return new TransferPkg { cmdId = -2 };
            }
        }

        public void ping_and_recv()
        {
            lock (lockobj)
            {
                try
                {
                    _logger.Trace("Pinging...");
                    clientSocket.Client.Poll(ping_timeout_in_millsec, SelectMode.SelectRead);
                    next_ping_deadline = DateTime.Now.AddSeconds(ping_interval_in_sec);
                }
                catch (Exception ex)
                {
                    _logger.Error(ex, "Error occured pinging");
                    reconnect();
                }
            }
        }

        TransferPkg build_TransferPkg(int message_map, byte[] data)
        {
            TransferPkg transfer_pkg = new TransferPkg();
            transfer_pkg.seqId = 0;
            transfer_pkg.cmdId = message_map;
            transfer_pkg.data = data;
            transfer_pkg.zip = false;
            transfer_pkg.ver = 0;

            return transfer_pkg;
        }

        public void Dispose()
        {
            isConnected = false;
            epoch = 0;
            if (clientSocket != null)
            {
                clientSocket.Dispose();
            }
            if (stream != null)
                stream.Dispose();
            if (queue != null)
                queue.Dispose();
        }
    }
}
