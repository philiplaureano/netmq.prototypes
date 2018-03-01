using System;
using NetMQ;
using NetMQ.Sockets;

namespace HubNode
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                var publisherAddress = "@tcp://127.0.0.1:8675";
                var subscriberAddress = "@tcp://127.0.0.1:5309";
                using (var xpubSocket = new XPublisherSocket(publisherAddress))
                {
                    using (var xsubSocket = new XSubscriberSocket(subscriberAddress))
                    {
                        Console.WriteLine("Intermediary started, and waiting for messages");

                        // proxy messages between frontend / backend
                        var proxy = new Proxy(xsubSocket, xpubSocket);

                        // blocks indefinitely
                        proxy.Start();
                    }
                }

                Console.WriteLine("Press ENTER to terminate the program");
                Console.ReadLine();
            }
            finally
            {
                NetMQConfig.Cleanup();
            }
        }
    }
}