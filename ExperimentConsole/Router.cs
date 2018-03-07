using System;
using System.Threading;
using System.Threading.Tasks;
using NetMQ;
using NetMQ.Sockets;

namespace ExperimentConsole
{
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
}