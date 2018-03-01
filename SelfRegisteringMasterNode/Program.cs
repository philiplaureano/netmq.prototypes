using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Common;
using Common.Messages;
using NetMQ;
using NetMQ.Sockets;
using NFX.Serialization.Slim;

namespace SelfRegisteringMasterNode
{
    class Program
    {
        static void Main(string[] args)
        {
            var publisherSocketAddress = ">tcp://127.0.0.1:5309";
            var subscriberSocketAddress = ">tcp://127.0.0.1:8675";

            var source = new CancellationTokenSource();
            try
            {
                var currentIpAddress = GetLocalIPAddress();

                BroadcastServiceAvailability(publisherSocketAddress, currentIpAddress);

                // Send out the availability message whenever a service query is made
               
                var responseTask = Task.Run(() => RespondToServiceQueries(publisherSocketAddress, subscriberSocketAddress), source.Token);

                // Note: Use the sender address as the topic for exclusive replies                   
                while (true)
                {
                    Console.WriteLine("Press enter when the workers are ready");
                    Console.ReadLine();
                
                    Console.WriteLine("Sending tasks to workers");
                    
                    using (var sender = new PushSocket("@tcp://*:5557"))
                    {
                        var maxValue = 1000;
                        var rand = new Random();
                        for (var i = 0; i < 100; i++)
                        {
                            var message = new CalculateFibonacci(rand.Next(maxValue));

                            // Convert the message into a byte stream
                            var stream = new MemoryStream();
                            var serializer = new SlimSerializer();
                            serializer.Serialize(stream, message);

                            // Send the message off to the worker nodes
                            var bytes = stream.ToArray();
                            sender.SendFrame(bytes);
                        }
                    }
                    
                    Console.WriteLine("Press Enter to quit, or press any other key to repeat the run");
                    var key = Console.ReadKey();

                    if (key.Key == ConsoleKey.Enter)
                        break;    
                }
            }
            finally
            {
                source.Cancel();
                NetMQConfig.Cleanup();
            }
        }

        private static async Task RespondToServiceQueries(string publisherSocketAddress, string subscriberSocketAddress)
        {
            var targetTopic = "Services";
            using (var subSocket = new SubscriberSocket(subscriberSocketAddress))
            {
                subSocket.Options.ReceiveHighWatermark = 1000;                
                subSocket.Subscribe(targetTopic);
                
                while (true)
                {
                    var currentTopic = subSocket.ReceiveFrameString();
                    var messageBytes = subSocket.ReceiveFrameBytes();
                    if (currentTopic != targetTopic)
                        continue;

                    var serviceQuery = messageBytes.Deserialize<QueryAvailableServices>();
                    if (serviceQuery == null)
                        continue;
                    
                    BroadcastServiceAvailability(publisherSocketAddress, GetLocalIPAddress());
                }
            }
        }
        
        private static void BroadcastServiceAvailability(string publisherSocketAddress, string currentIpAddress)
        {
            using (var pubSocket = new PublisherSocket(publisherSocketAddress))
            {
                // Broadcast the availability of the current node
                // using the current IP address
                var serviceId = 42;
                var serviceAvailable = new ServiceAvailable(serviceId, currentIpAddress, 5557);

                var stream = new MemoryStream();
                var serializer = new SlimSerializer();
                serializer.Serialize(stream, serviceAvailable);

                pubSocket.SendMoreFrame("Services").SendFrame(stream.ToArray());
            }
        }

        private static string GetLocalIPAddress()
        {
            var host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (var ip in host.AddressList)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    return ip.ToString();
                }
            }

            throw new Exception("No network adapters with an IPv4 address in the system!");
        }
    }
}