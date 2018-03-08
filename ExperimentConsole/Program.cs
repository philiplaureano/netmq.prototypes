using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.NetworkInformation;
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

    public class NetworkNode : IDisposable, INetworkNode, IMessageHandler
    {
        private readonly Router _router;
        private readonly Action<object> _messageHandler;

        public NetworkNode(string address, Action<object> messageHandler)
        {
            _messageHandler = messageHandler;
            Address = address;
            _router = new Router(address, OnClientRequestReceived);
        }

        public async Task Run(CancellationToken token)
        {
            await _router.Run(token);
        }

        public string Address { get; }

        public string ID => _router.Identity;

        public void SendMessage(object message)
        {
            _messageHandler?.Invoke(message);
        }

        private void OnClientRequestReceived(string serverId, string clientId,
            byte[] clientMessageBytes)
        {
            var request = new Request(serverId, clientId, clientMessageBytes);

            _messageHandler?.Invoke(request);
        }

        public void Dispose()
        {
            _router?.Dispose();
        }
    }

    public interface IMessageHandler
    {
        void SendMessage(object message);
    }

    public struct Message
    {
        public Message(string targetId, byte[] bytes)
        {
            TargetId = targetId ?? throw new ArgumentNullException(nameof(targetId));
            Bytes = bytes ?? throw new ArgumentNullException(nameof(bytes));
        }

        public string TargetId { get; }
        public byte[] Bytes { get; }
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
        private readonly Action<object> _sendMessage;

        public PingPongActor(Action<object> sendMessage)
        {
            _sendMessage = sendMessage;
        }

        public void SendMessage(object message)
        {
            if (message is string msg)
            {
                Console.WriteLine($"'{msg}' message received");
            }

            if (message is PingMessage p)
            {
                var serverId = p.TargetId;
                var serverMessage = new Message(serverId, Encoding.UTF8.GetBytes("Ping"));
                _sendMessage?.Invoke(serverMessage);
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
                var serverMessage = new Message(serverId, Encoding.UTF8.GetBytes("Pong"));
                _sendMessage?.Invoke(serverMessage);

                Console.WriteLine($"Ping message received from client '{request.ClientId}'");
            }
        }
    }
   
    public class InMemoryNode : INode, IMessageHandler
    {
        private readonly IMessageHandler _messageHandler;
        public InMemoryNode(IMessageHandler messageHandler)
        {
            _messageHandler = messageHandler;
            ID = Guid.NewGuid().ToString();
        }

        public string ID { get; }

        public void SendMessage(object message)
        {
            _messageHandler?.SendMessage(message);
        }        
    }

    // Note: This is just a console app where I will play around with
    // some sample code. None of it is meant for production use.
    class Program
    {
        static void Main(string[] args)
        {
            var nodes = new ConcurrentDictionary<string, IMessageHandler>();
            Action<object> sendMessage = msg =>
            {
                if (!(msg is Message m))
                    return;

                var serverId = m.TargetId;
                if (!nodes.ContainsKey(serverId))
                    return;

                var messageText = Encoding.UTF8.GetString(m.Bytes);
                nodes[serverId]?.SendMessage(messageText);
            };

            var node1 = new InMemoryNode(new PingPongActor(sendMessage));
            var node2 = new InMemoryNode(new PingPongActor(sendMessage));

            nodes[node1.ID] = node1;
            nodes[node2.ID] = node2;

            node1.SendMessage(new PingMessage(node2.ID));

            Console.WriteLine("Press ENTER to terminate the program");
            Console.ReadLine();
        }

        private static void RunNetworkNodeDemo()
        {
            var source = new CancellationTokenSource();

            var addresses = new ConcurrentDictionary<string, string>();
            var dealers = new ConcurrentDictionary<string, Dealer>();
            var handlers = new ConcurrentDictionary<string, IMessageHandler>();
            
            Action<object> sendMessage = msg =>
            {
                if (!(msg is Message m))
                    return;

                var serverId = m.TargetId;
                if (!dealers.ContainsKey(serverId) && addresses.ContainsKey(serverId))
                {
                    var socketAddress = addresses[serverId];
                    dealers[serverId] = new Dealer(socketAddress, (currentServerId, clientId, socket) =>
                    {
                        var serverMessageBytes = socket.ReceiveFrameBytes();
                        var response = new Response(serverId, clientId, serverMessageBytes);

                        if (!handlers.ContainsKey(serverId))
                            return;
                        
                        handlers[serverId]?.SendMessage(response);
                    });
                }

                if (!dealers.ContainsKey(serverId))
                    return;
                
                dealers[serverId]?.SendMessage(m.Bytes);
            };

            var actor1 = new PingPongActor(sendMessage);
            var actor2 = new PingPongActor(sendMessage);
            
            var node1 = new NetworkNode("inproc://node-1", actor1.SendMessage);
            var node2 = new NetworkNode("inproc://node-2", actor2.SendMessage);

            addresses[node1.ID] = node1.Address;
            addresses[node2.ID] = node2.Address;

            handlers[node1.ID] = actor1;
            handlers[node2.ID] = actor2;
            
            var tasks = new Task[]
            {
                Task.Run(() => node1.Run(source.Token), source.Token),
                Task.Run(() => node2.Run(source.Token), source.Token)
            };

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