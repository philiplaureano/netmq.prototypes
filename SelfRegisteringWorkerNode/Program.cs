using System;
using System.Threading.Tasks;
using Common;
using Common.Messages;
using NetMQ;
using NetMQ.Sockets;
using NFX.DataAccess.CRUD;

namespace SelfRegisteringWorkerNode
{
    class Program
    {
        static void Main(string[] args)
        {
            var subscriptionAddress = ">tcp://127.0.0.1:8675";
            var publisherAddress = ">tcp://127.0.0.1:5309";

            Task.Run(() => ProcessMessages(subscriptionAddress));
            
            // Send out the QueryAvailableServices message
            // to get the list of available master nodes
            var query = new QueryAvailableServices();
            query.Publish("Services",publisherAddress);

            // Connect to a master node
            // whenever a service availability message
            // is received
            
            Console.WriteLine("Press ENTER to exit");
            Console.ReadLine();
        }

        private static void ProcessMessages(string subscriptionAddress)
        {
            using (var subSocket = new SubscriberSocket(subscriptionAddress))
            {
                subSocket.Options.ReceiveHighWatermark = 1000;
                subSocket.Subscribe("Services");
               
                while (true)
                {
                    var topic = subSocket.ReceiveFrameString();
                    var bytes = subSocket.ReceiveFrameBytes();
                    
                    var message = bytes.Deserialize<object>();

                    if (message != null && message is ServiceAvailable availableMessage)
                    {
                        Console.WriteLine($"Host Available: {availableMessage.HostName}, port {availableMessage.Port}");
                    }                    
                }
            }
        }
    }
}