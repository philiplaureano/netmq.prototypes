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
        private readonly string _socketAddress;
        private readonly Action<string, string, byte[]> _handleRequest;
        private readonly RouterSocket _routerSocket;

        public Router(string socketAddress,
            Action<string, string, byte[]> handleRequest)
            : this(Guid.NewGuid().ToString(), socketAddress, handleRequest)
        {
        }

        public Router(string identity, string socketAddress,
            Action<string, string, byte[]> handleRequest)
        {
            _identity = identity;
            _socketAddress = socketAddress;
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
                var clientId = clientMessage[0].ConvertToString();
                var originalMessage = clientMessage[2].ToByteArray();

                _handleRequest?.Invoke(_identity, clientId, originalMessage);
            }

            return Task.FromResult(0);
        }

        public string Identity => _identity;

        public void Dispose()
        {
            _routerSocket?.Close();
            _routerSocket?.Dispose();
        }
    }
}