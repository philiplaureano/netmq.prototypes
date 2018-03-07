﻿using System;
using System.Collections.Generic;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetMQ;
using NetMQ.Sockets;

namespace ExperimentConsole
{
    public class NetworkNode : IDisposable
    {
        private Dealer _dealer;
        private Router _router;
        private Guid _dealerId = Guid.NewGuid();
        private Guid _routerId = Guid.NewGuid();

        public NetworkNode(string address)
        {
            _dealer = new Dealer(_dealer.ToString(), OnRemoteResponseReceived);
            _router = new Router(_router.ToString(), address, OnClientRequestReceived);
        }

        public async Task Run(CancellationToken token)
        {
            await _router.Run(token);
        }

        protected void SendMessage(string socketAddress, byte[] messageBytes)
        {
            _dealer.SendMessage(socketAddress, messageBytes);
        }

        private void OnClientRequestReceived(string dealerId, string clientId,
            byte[] clientMessageBytes, Action<byte[]> sendResponse)
        {
            // TODO: Process the client (dealer) requests here
            return;
        }

        private void OnRemoteResponseReceived(string serverId, IReceivingSocket socket)
        {
            // TODO: Process the server (router) responses here
            return;
        }

        public void Dispose()
        {
            _dealer?.Dispose();
            _router?.Dispose();
        }
    }

    // Note: This is just a console app where I will play around with
    // some sample code. None of it is meant for production use.
    class Program
    {
        static void Main(string[] args)
        {
            // TODO: Simulate a node with dealer (client) and router (server) sockets
            RunDealerRouterDemo();
        }

        private static void RunDealerRouterDemo()
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
                (identity, clientId, clientMessage, sendResponse) =>
                {
                    Console.WriteLine(
                        $"Message received from dealer '{identity}': {Encoding.UTF8.GetString(clientMessage)}");
                    sendResponse(Encoding.UTF8.GetBytes("Pong"));
                };

            var router = new Router(Guid.NewGuid().ToString(), serverAddress, handleRequest);
            var otherRouter = new Router(Guid.NewGuid().ToString(), "inproc://other-server", handleRequest);

            var routerTasks = new Task[]
            {
                Task.Run(() => router.Run(source.Token), source.Token),
                Task.Run(() => otherRouter.Run(source.Token), source.Token)
            };

            var dealers = new List<Dealer>();
            for (var i = 0; i < 100; i++)
            {
                var dealer = new Dealer(Guid.NewGuid().ToString(), receiveReady);
                dealers.Add(dealer);
            }

            for (var i = 0; i < 10; i++)
            {
                var dealer = dealers.GetRandomElement();
                dealer.SendMessage(serverAddress, Encoding.UTF8.GetBytes($"Ping-{i}"));
                dealer.SendMessage("inproc://other-server", Encoding.UTF8.GetBytes($"Pang-{i}"));
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