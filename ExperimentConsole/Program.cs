using System;
using System.Collections.Generic;
using System.Linq;
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
        private readonly Action<string, Guid,IReceivingSocket> _receiveMessages;
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

    // Note: This is just a console app where I will play around with
    // some sample code. None of it is meant for production use.
    class Program
    {
        static void Main(string[] args)
        {
            var source = new CancellationTokenSource();
            
            // TODO: Create the dealer/router prototype here
            // Note: Dealers are the clients; routers are the servers
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