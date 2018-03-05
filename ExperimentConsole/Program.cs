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
        private PublisherSocket _pubSocket;

        public Publisher(string publisherAddress)
        {
            _pubSocket = new PublisherSocket(publisherAddress);
        }

        public void Publish(string topic, byte[] message)
        {
            _pubSocket.Options.SendHighWatermark = 1000;
            _pubSocket.SendMoreFrame(topic).SendFrame(message);
        }

        public void Dispose()
        {
            _pubSocket?.Close();
            _pubSocket?.Dispose();
        }
    }

    public class Subscriber
    {
        private readonly Action<string, IReceivingSocket> _receiveMessages;

        public Subscriber(Action<string, IReceivingSocket> receiveMessages)
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

                Console.WriteLine("Subscriber socket connecting...");

                while (true)
                {
                    if (token.IsCancellationRequested)
                        break;

                    var topic = subSocket.ReceiveFrameString();
                    _receiveMessages(topic, subSocket);
                }
            }

            return Task.FromResult(0);
        }
    }

    // Note: This is just a console app where I play around with
    // some sample code. None of it is meant for production use.
    class Program
    {
        static void Main(string[] args)
        {
            var source = new CancellationTokenSource();

            Task.Run(() => StartProxy());

            Console.WriteLine("Press any key to start the subscriber");
            Console.ReadKey();
            
            var eventTargetAddress = ">inproc://subscriber";
            var eventSourceAddress = ">inproc://publisher";

            var publisher = new Publisher(eventTargetAddress);

            // Start listening for messages
            var subscriber = new Subscriber(HandleMessages);
            var topics = new[] {"TopicA", "TopicB", "TopicC"};

            var listenerTask = Task.Run(() => subscriber.Run(eventSourceAddress, topics, source.Token));
            
            Console.WriteLine("Press any key to start publishing the messages");
            Console.ReadKey();

            var tasks = new List<Task>();
            for (var i = 0; i < 1000; i++)
            {
                var topic = "TopicB";
               
                var message = $"Message{i:0000}";
                var bytes = Encoding.UTF8.GetBytes(message);
                publisher.Publish(topic, bytes);
            }

            Task.WaitAll(tasks.ToArray());
            
            Console.WriteLine("Press ENTER to terminate the program");
            Console.ReadLine();

            source.Cancel();
        }

        private static void HandleMessages(string topic, IReceivingSocket socket)
        {
            var bytes = socket.ReceiveFrameBytes();
            var messageText = Encoding.UTF8.GetString(bytes);

            Console.WriteLine($"[Topic: '{topic}' ] Message Received: {messageText}");
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