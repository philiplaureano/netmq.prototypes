using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetMQ;
using NetMQ.Sockets;

namespace ExperimentConsole
{
    public interface INode
    {
        string ID { get; }
        void SendMessage(object message);
    }

    public interface INetworkNode : INode
    {
        string Address { get; }
    }

    public struct Request
    {
        public Request(string serverId, string clientId, byte[] bytes)
        {
            ServerId = serverId;
            ClientId = clientId;
            Bytes = bytes;
        }

        public string ServerId { get; }
        public string ClientId { get; }
        public byte[] Bytes { get; }
    }

    public struct Response
    {
        public Response(string serverId, string clientId, byte[] bytes)
        {
            ServerId = serverId;
            ClientId = clientId;
            Bytes = bytes;
        }

        public string ServerId { get; }
        public string ClientId { get; }
        public byte[] Bytes { get; }
    }

    public class NetworkNode : IDisposable, INetworkNode
    {
        private readonly Router _router;
        private readonly Action<object, Action<string, byte[]>> _messageHandler;
        private readonly ConcurrentDictionary<string, Dealer> _dealers = new ConcurrentDictionary<string, Dealer>();

        public NetworkNode(string address, Action<object, Action<string, byte[]>> messageHandler)
        {
            _messageHandler = messageHandler;
            Address = address;
            _router = new Router(address, OnClientRequestReceived);

            // Add the loopback dealer
            _dealers[_router.Identity] = new Dealer(address, OnServerResponseReceived);
        }

        public async Task Run(CancellationToken token)
        {
            await _router.Run(token);
        }

        public string Address { get; }

        public string ID => _router.Identity;

        public void ConnectTo(INetworkNode otherNode)
        {
            if (otherNode.ID == this.ID)
                throw new InvalidOperationException("A node cannot connect to itself.");

            if (_dealers.ContainsKey(otherNode.ID))
                return;

            var serverId = otherNode.ID;
            _dealers[serverId] = new Dealer(otherNode.Address, OnServerResponseReceived);
            _dealers[serverId].Connect();
        }

        public void SendMessage(object message)
        {
            _messageHandler?.Invoke(message, SendMessage);
        }

        private void SendMessage(string serverId, byte[] message)
        {
            if (!_dealers.ContainsKey(serverId))
                throw new ArgumentException($"ServerId '{serverId}' not found");

            var currentDealer = _dealers[serverId];
            currentDealer.SendMessage(message);
        }

        private void OnClientRequestReceived(string serverId, string clientId,
            byte[] clientMessageBytes)
        {
            var request = new Request(serverId, clientId, clientMessageBytes);

            _messageHandler?.Invoke(request, SendMessage);
        }

        private void OnServerResponseReceived(string serverId, string clientId, IReceivingSocket socket)
        {
            var serverMessageBytes = socket.ReceiveFrameBytes();
            var response = new Response(serverId, clientId, serverMessageBytes);

            _messageHandler?.Invoke(response, SendMessage);
        }

        public void Dispose()
        {
            _router?.Dispose();

            foreach (var dealer in _dealers.Values)
            {
                dealer.Dispose();
            }
        }
    }

    public interface IMessageHandler
    {
        void HandleMessage(object message, Action<string, byte[]> sendMessage);
    }

    public struct PingMessage
    {
        public PingMessage(string targetId)
        {
            TargetId = targetId;
        }

        public string TargetId { get; }
    }

    public class PingPongActor : IMessageHandler
    {
        public void HandleMessage(object message, Action<string, byte[]> sendMessage)
        {
            if (message is PingMessage p)
            {
                var serverId = p.TargetId;
                sendMessage(serverId, Encoding.UTF8.GetBytes("Ping"));
            }

            if (message is Response response)
            {
                var messageText = Encoding.UTF8.GetString(response.Bytes);
                Console.WriteLine($"Message received from server '{response.ServerId}': {messageText}");
            }

            if (message is Request request)
            {
                var text = Encoding.UTF8.GetString(request.Bytes);
                if (text != "Ping")
                    return;

                var serverId = request.ServerId;
                sendMessage(serverId, Encoding.UTF8.GetBytes("Pong"));

                Console.WriteLine($"Ping message received from client '{request.ClientId}'");
            }
        }
    }

    public static class MessageHandlerExtensions
    {
        public static NetworkNode CreateNetworkNode(this IMessageHandler handler, string address)
        {
            return new NetworkNode(address, handler.HandleMessage);
        }
    }

    public class InMemoryNode : INode
    {
        private readonly IMessageHandler _messageHandler;
        private readonly Func<byte[], object> _messageParser;

        public InMemoryNode(IMessageHandler messageHandler, Func<byte[], object> messageParser)
        {
            _messageHandler = messageHandler;
            _messageParser = messageParser;
            ID = Guid.NewGuid().ToString();
        }

        public string ID { get; }

        public void SendMessage(object message)
        {
            _messageHandler?.HandleMessage(message, SendMessage);
        }

        private void SendMessage(string targetId, byte[] messageBytes)
        {
            var message = _messageParser?.Invoke(messageBytes);
            if (targetId == ID)
            {
                SendMessage(message);
                return;
            }

            NextNode?.SendMessage(message);
        }

        public INode NextNode { get; set; }
    }

    // Note: This is just a console app where I will play around with
    // some sample code. None of it is meant for production use.
    class Program
    {
        static void Main(string[] args)
        {
            // TODO: Add the in-memory transport node for the message handlers
        }

        private static void RunNetworkNodeDemo()
        {
            var source = new CancellationTokenSource();

            var node1 = (new PingPongActor()).CreateNetworkNode("inproc://node-1");
            var node2 = (new PingPongActor()).CreateNetworkNode("inproc://node-2");

            var tasks = new Task[]
            {
                Task.Run(() => node1.Run(source.Token), source.Token),
                Task.Run(() => node2.Run(source.Token), source.Token)
            };
            node1.ConnectTo(node2);
            node1.SendMessage(new PingMessage(node2.ID));

            Console.WriteLine("Press ENTER to terminate the program");
            Console.ReadLine();
        }

        private static void RunXPubXSubDemo()
        {
            var source = new CancellationTokenSource();

            Task.Run(() => StartProxy(), source.Token);

            Console.WriteLine("Press any key to start the subscriber");
            Console.ReadKey();

            var eventTargetAddress = ">inproc://subscriber";
            var eventSourceAddress = ">inproc://publisher";

            var publisher = new Publisher(eventTargetAddress);

            // Start listening for messages
            var topics = new[] {"TopicA", "TopicB", "TopicC"};

            CreateSubscriber(eventSourceAddress, "TopicA", source);
            CreateSubscriber(eventSourceAddress, "TopicB", source);
            CreateSubscriber(eventSourceAddress, "TopicC", source);

            Console.WriteLine("Press any key to start publishing the messages");
            Console.ReadKey();

            var random = new Random();
            var tasks = new List<Task>();
            for (var i = 0; i < 1000; i++)
            {
                var topic = topics[random.Next(0, topics.Length)];

                var message = $"Message{i:0000}";
                var bytes = Encoding.UTF8.GetBytes(message);
                publisher.Publish(topic, bytes);
            }

            Task.WaitAll(tasks.ToArray());

            Console.WriteLine("Press ENTER to terminate the program");
            Console.ReadLine();

            source.Cancel();
        }

        private static Task CreateSubscriber(string eventSourceAddress, string topic, CancellationTokenSource source)
        {
            return CreateSubscriber(eventSourceAddress, new[] {topic}, source);
        }

        private static Task CreateSubscriber(string eventSourceAddress, string[] topics, CancellationTokenSource source)
        {
            var subscriber = new Subscriber(HandleMessages);
            var listenerTask = Task.Run(() => subscriber.Run(eventSourceAddress, topics, source.Token));
            return listenerTask;
        }

        private static void HandleMessages(string topic, Guid subscriberId, IReceivingSocket socket)
        {
            var bytes = socket.ReceiveFrameBytes();
            var messageText = Encoding.UTF8.GetString(bytes);

            Console.WriteLine($"[SubscriberId {subscriberId}][Topic: '{topic}' ] Message Received: {messageText}");
        }

        private static void StartProxy()
        {
            using (var xpubSocket = new XPublisherSocket("@inproc://publisher"))
            using (var xsubSocket = new XSubscriberSocket("@inproc://subscriber"))
            {
                Console.WriteLine("Intermediary started, and waiting for messages");

                // proxy messages between frontend / backend
                var proxy = new Proxy(xsubSocket, xpubSocket);

                // blocks indefinitely
                proxy.Start();
            }
        }
    }
}