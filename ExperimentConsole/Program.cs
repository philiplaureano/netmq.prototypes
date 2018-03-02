using System;
using System.Threading.Tasks;
using NetMQ;
using NetMQ.Sockets;

namespace ExperimentConsole
{
    // Note: This is just a console app where I play around with
    // some sample code. None of it is meant for production use.
    class Program
    {
        static void Main(string[] args)
        {
            Task.Run(() => StartProxy());

            Console.WriteLine("Press ENTER to terminate the program");
            Console.ReadLine();
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