using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetMQ;
using NetMQ.Sockets;

namespace ExperimentConsole
{
    public class Publisher : IDisposable
    {
        private readonly IOutgoingSocket _pubSocket;

        public Publisher(string publisherAddress)
        {
            var pubSocket = new PublisherSocket(publisherAddress);
            pubSocket.Options.SendHighWatermark = 1000;

            _pubSocket = pubSocket;
        }

        public void Publish(string topic, byte[] message)
        {
            _pubSocket.SendMoreFrame(topic).SendFrame(message);
        }

        public void Dispose()
        {
            var pubSocket = _pubSocket as IDisposable;
            pubSocket?.Dispose();
        }
    }

    public class Subscriber
    {
        private readonly Action<string, Guid, IReceivingSocket> _receiveMessages;
        private readonly Guid _subscriberId = Guid.NewGuid();

        public Subscriber(Action<string, Guid, IReceivingSocket> receiveMessages)
        {
            _receiveMessages = receiveMessages;
        }

        public Task Run(string subscriberSocketAddress, IEnumerable<string> topics,
            CancellationToken token)
        {
            using (var subSocket = new SubscriberSocket(subscriberSocketAddress))
            {
                subSocket.Options.ReceiveHighWatermark = 1000;
                foreach (var topic in topics)
                {
                    subSocket.Subscribe(topic);
                }

                Console.WriteLine($"Subscriber '{_subscriberId}' connecting...");

                while (true)
                {
                    if (token.IsCancellationRequested)
                        break;

                    var topic = subSocket.ReceiveFrameString();
                    _receiveMessages(topic, _subscriberId, subSocket);
                }
            }

            return Task.FromResult(0);
        }
    }

    public class Dealer : IDisposable
    {
        private DealerSocket _dealerSocket;
        private readonly Action<string, IReceivingSocket> _receiveReady;
        private readonly NetMQPoller _poller = new NetMQPoller();
        private readonly string _identity;
        private readonly string _socketAddress;

        public Dealer(string identity, string socketAddress, Action<string, IReceivingSocket> receiveReady)
        {
            _receiveReady = receiveReady;
            _identity = identity;
            _socketAddress = socketAddress;
        }

        public void SendMessage(byte[] messageBytes)
        {
            if (_dealerSocket == null)
            {
                InitializeSocket();
                _dealerSocket.Connect(_socketAddress);
            }

            // The first frame must be empty,
            // followed by the message itself
            _dealerSocket.SendMoreFrameEmpty()
                .SendFrame(messageBytes);
        }

        private void InitializeSocket()
        {
            if (_dealerSocket != null)
                return;

            _dealerSocket = new DealerSocket();
            _dealerSocket.Options.Identity = Encoding.UTF8.GetBytes(_identity);
            _dealerSocket.ReceiveReady += OnReceiveReady;

            _poller.Add(_dealerSocket);
            _poller.RunAsync();
        }

        private void OnReceiveReady(object sender, NetMQSocketEventArgs e)
        {
            var socket = e.Socket;

            // Discard the whole message if the first frame isn't empty
            var firstFrame = socket?.ReceiveFrameBytes();
            if (firstFrame?.Length != 0)
                return;

            // Pass the remaining message to the application
            _receiveReady(_identity, socket);
        }

        public void Dispose()
        {
            _dealerSocket?.Dispose();
            _poller?.Dispose();
        }
    }

    public class Router : IDisposable
    {
        private readonly string _identity;
        private readonly Action<string, string, byte[], Action<byte[]>> _handleRequest;
        private readonly RouterSocket _routerSocket;

        public Router(string identity, string socketAddress,
            Action<string, string, byte[], Action<byte[]>> handleRequest)
        {
            _identity = identity;
            _handleRequest = handleRequest;
            _routerSocket = new RouterSocket(socketAddress);
        }

        public Task Run(CancellationToken token)
        {
            while (true)
            {
                if (token.IsCancellationRequested)
                    break;

                var clientMessage = _routerSocket.ReceiveMultipartMessage(3);
                if (clientMessage.FrameCount < 3)
                    continue;

                // Call the handler and give it the option to send responses back
                // to the client
                var clientAddress = clientMessage[0].ConvertToString();
                var originalMessage = clientMessage[2].ToByteArray();
                Action<byte[]> sendResponse = bytes =>
                {
                    _routerSocket.SendMoreFrame(clientAddress)
                        .SendMoreFrameEmpty().SendFrame(bytes);
                };

                _handleRequest?.Invoke(_identity, clientAddress, originalMessage, sendResponse);
            }

            return Task.FromResult(0);
        }

        public void Dispose()
        {
            _routerSocket?.Close();
            _routerSocket?.Dispose();
        }
    }

    public static class ArrayExtensions
    {
        public static TItem GetRandomElement<TItem>(this IEnumerable<TItem> items)
        {
            // var topic = topics[random.Next(0, topics.Length)];
            var random = new Random();
            var itemsAsArray = items.ToArray();

            return itemsAsArray.Length == 0 ? default(TItem) : itemsAsArray[random.Next(0, itemsAsArray.Length)];
        }
    }
    // Note: This is just a console app where I will play around with
    // some sample code. None of it is meant for production use.
    class Program
    {
        static void Main(string[] args)
        {
            var source = new CancellationTokenSource();

            // Note: Dealers are the clients; routers are the servers
            var serverAddress = "inproc://server";
            Action<string, IReceivingSocket> receiveReady = (dealerId, dealerSocket) =>
            {
                var message = dealerSocket.ReceiveFrameBytes();
                var messageText = Encoding.UTF8.GetString(message);

                Console.WriteLine($"Message received from router '{dealerId}': {messageText}");
            };

            Action<string, string, byte[], Action<byte[]>> handleRequest =
                (identity, sourceAddress, clientMessage, sendResponse) =>
                {
                    Console.WriteLine(
                        $"Message received from dealer '{identity}': {Encoding.UTF8.GetString(clientMessage)}");
                    sendResponse(Encoding.UTF8.GetBytes("Pong"));
                };

            var router = new Router(Guid.NewGuid().ToString(), serverAddress, handleRequest);
            var routerTask = Task.Run(() => router.Run(source.Token), source.Token);

            var dealers = new List<Dealer>();
            for (var i = 0; i < 100; i++)
            {
                var dealer = new Dealer(Guid.NewGuid().ToString(), serverAddress, receiveReady);
                dealers.Add(dealer);
            }

            for (var i = 0; i < 10; i++)
            {
                var dealer = dealers.GetRandomElement();
                dealer.SendMessage(Encoding.UTF8.GetBytes($"Ping-{i}"));    
            }            

            Console.WriteLine("Press ENTER to terminate the program");
            Console.ReadLine();

            source.Cancel();
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