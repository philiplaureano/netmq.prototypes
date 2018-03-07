using System;
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
}