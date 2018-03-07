using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NetMQ;
using NetMQ.Sockets;

namespace ExperimentConsole
{
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
}