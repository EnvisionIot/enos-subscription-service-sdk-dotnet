using enos_subscription_service.proto;
using enos_subscription_service.util;
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

namespace enos_subscription_service.core
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
        private int requestTimeout { get; set; } = 30000;
        public int pull_id { get; set; } = 0;
        public int epoch { get; set; } = 0;
        public DateTime next_ping_deadline { get; set; }
        private readonly object lockobj = new object();

        private readonly int ping_interval_in_sec = 10;
        private readonly int ping_timeout_in_millsec = 500;
        private readonly int DEFAULT_MESSAGE_QUEUE_SIZE = 100;
        private readonly int message_max_size = 10000000;

        private static NLog.Logger _logger = NLog.LogManager.GetCurrentClassLogger();
        private BlockingCollection<Message> queue { get; set; } = null;
        internal bool isConnected = false;
        internal Socket clientSocket = null;

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
            next_ping_deadline = DateTime.Now.AddSeconds(ping_interval_in_sec);
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

                    clientSocket = new Socket(remoteEP.AddressFamily,
                        SocketType.Stream, ProtocolType.Tcp);

                    //IPEndPoint myEP = new IPEndPoint(IPAddress.Any, 12345);
                    //clientSocket.Bind(myEP);
                    //clientSocket.ReceiveTimeout = 3000;
                    //clientSocket.ReceiveBufferSize = 0;message_max_size;
                    clientSocket.Connect(remoteEP);
                    //clientSocket.Accept();

                    Thread.Sleep(1000);
                    //isConnected = clientSocket.Connected;
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

            byte[] msg = build_pkg((int)CmdId.AuthReq, ProtoBufEncoder.SerializeToBytes(auth_req));

            TransferPkg auth_res = send_and_recv(msg);
            if (auth_res == null)
            {
                _logger.Error("Auth fail, receive unexpect response, need AuthRsp, receive nothing.");
                reconnect();
                return;
            }
            //check if auth is successful
            if (auth_res.cmdId == (int)CmdId.AuthRsp)
            {
                AuthRsp rsp = ProtoBufDecoder.DeserializeToObj<AuthRsp>(auth_res.data);
                if (rsp.ack == 0)
                {
                    _logger.Info("auth successfully.");
                    //new thread to ping server to keep it alive
                    LifeKeeper lk = new LifeKeeper();
                    Thread life_keeper = new Thread(() => lk.Run(this));
                    life_keeper.Start();
                    //sub
                    sub();
                }
                else
                {
                    throw new Exception("Auth fail, auth info:" + auth_req.ToString());
                }
            }
            else
            {
                throw new Exception("Auth fail, receive unexpect response, need AuthRsp, receive:" + auth_res.cmdId);
            }
        }

        void sub()
        {
            SubReq sub_req = new SubReq();

            sub_req.category = subType;
            sub_req.clientId = ((IPEndPoint)(clientSocket.LocalEndPoint)).Address.ToString();
            sub_req.subId = sub_id;
            sub_req.accessKey = accessKey;
            sub_req.consumerGroup = consumer_group;

            byte[] msg = build_pkg((int)CmdId.SubReq, ProtoBufEncoder.SerializeToBytes(sub_req));

            TransferPkg sub_res = send_and_recv(msg);

            //check if subscription is successful
            if (sub_res == null)
            {
                _logger.Error("Sub fail, receive unexpect response, need SubRsp, receive nothing.");
                reconnect();
                return;
            }
            if (sub_res.cmdId == (int)CmdId.SubRsp)
            {
                SubRsp rsp = ProtoBufDecoder.DeserializeToObj<SubRsp>(sub_res.data);
                if (rsp.ack == 0)
                {
                    isConnected = true;
                    //new thread to run fetching
                    //Thread.Sleep(2000);
                    SubFetcher fetcher = new SubFetcher();
                    Thread fetchthread = new Thread(() => fetcher.Run(this));
                    fetchthread.Start();
                }
                else
                {
                    throw new Exception("Sub fail, sub info:" + sub_req.ToString());
                }
            }
            else
            {
                throw new Exception("Sub fail, receive unexpect response, need SubRsp, receive:" + sub_res.cmdId);
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

            byte[] msg = build_pkg((int)CmdId.PullReq, ProtoBufEncoder.SerializeToBytes(pull_req));

            TransferPkg pull_res = send_and_recv(msg, true);
            if (pull_res == null)
            {
                _logger.Error("Pull fail, receive unexpect response, need PullRsp, receive nothing.");
                reconnect();
                return;
            }
            if (pull_res.cmdId == (int)CmdId.PullRsp)
            {
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
                _logger.Error("Pull fail, receive unexpect response, need PullRsp, receive " + pull_res.cmdId);
            }

        }

        public void commit_offsets(CommitDTO commit_dto)
        {
            if (!isConnected || commit_dto == null || commit_dto.commits.Count==0)
                return;

            _logger.Info("committing offset...");

            foreach (var commit in commit_dto.commits)
            {
                _logger.Info(string.Format("committing topic: {0}, partition: {1}, offset: {2}  ", commit.topic, commit.partition, commit.offset));
            }

            byte[] msg = build_pkg((int)CmdId.CommitReq, ProtoBufEncoder.SerializeToBytes(commit_dto));

            TransferPkg commit_res = send_and_recv(msg, false, false);
            if (commit_res == null)
            {
                _logger.Error("Commit fail, receive unexpect response, need CommitRsp, receive nothing.");
                reconnect();
                return;
            }
            if (commit_res.cmdId != -3)
            {
                _logger.Error("Error occur commiting offset, reconnecting...");
            }
        }
        public Message poll()
        {
            _logger.Trace("polling message...");
            return queue.Take();
        }
        TransferPkg send_and_recv(byte[] message, bool is_pull_req = false, bool need_recv = true)
        {
            lock (lockobj)
            {
                byte[] data = new byte[4];
                try
                {
                    clientSocket.Send(message);
                    if (need_recv)
                    {
                        data = receivePkg(is_pull_req);
                        string txt = Encoding.UTF8.GetString(data);
                        return ProtoBufDecoder.DeserializeToObj<TransferPkg>(data, true);
                    }
                    else
                    {
                        return new TransferPkg { cmdId = -3 };
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error("Error send and rev message: " + ex.Message);

                    //_logger.Info("received data: " + Encoding.UTF8.GetString(data));

                    if (ex is SocketException)
                    {
                        reconnect();
                    }
                }

                return new TransferPkg { cmdId = -2 };
            }
        }

        private byte[] receivePkg(bool is_pull = false)
        {
            if (is_pull)
            {
                int buffer_size = clientSocket.ReceiveBufferSize;
                List<byte> list = new List<byte>();
                while (true)
                {
                    byte[] buffer = new byte[buffer_size];
                    int rec = clientSocket.Receive(buffer, 0, buffer.Length, 0);
                    byte[] data = new byte[rec];
                    Array.Copy(buffer, data, rec);
                    list.AddRange(data);
                    if (rec > 0 && rec < buffer_size && buffer[rec - 1] == 0)
                        break;
                }
                return list.ToArray();
            }
            else
            {
                int buffer_size = 100;
                byte[] buffer = new byte[buffer_size];
                int rec = clientSocket.Receive(buffer);
                byte[] data = new byte[rec];
                Array.Copy(buffer, data, rec);
                return data;
            }
        }


        public void ping_and_recv()
        {
            lock (lockobj)
            {
                try
                {
                    _logger.Trace("Pinging...");
                    clientSocket.Poll(ping_timeout_in_millsec, SelectMode.SelectRead);
                    next_ping_deadline = DateTime.Now.AddSeconds(ping_interval_in_sec);
                }
                catch (Exception ex)
                {
                    _logger.Error(ex, "Error occur pinging");
                    if (ex is SocketException)
                    {
                        reconnect();
                    }
                    else
                        throw;
                }
            }
        }
        byte[] build_pkg(int message_map, byte[] data)
        {
            TransferPkg transfer_pkg = new TransferPkg();
            transfer_pkg.seqId = 0;
            transfer_pkg.cmdId = message_map;
            transfer_pkg.data = data;
            transfer_pkg.zip = false;
            transfer_pkg.ver = 0;

            return ProtoBufEncoder.SerializeToBytes(transfer_pkg, true);
        }

        public void Dispose()
        {
            isConnected = false;
            epoch = 0;
            if (clientSocket != null)
            {
                clientSocket.Dispose();
            }
            if (queue != null)
                queue.Dispose();
        }
    }
}
