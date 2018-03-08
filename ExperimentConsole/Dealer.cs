using System;
using System.Collections.Generic;
using System.Text;
using NetMQ;
using NetMQ.Sockets;

namespace ExperimentConsole
{
    public class Dealer : IDisposable
    {
        private DealerSocket _dealerSocket;
        private readonly Action<string, string, IReceivingSocket> _receiveReady;
        private readonly NetMQPoller _poller = new NetMQPoller();
        private readonly string _identity;
        private readonly string _socketAddress;
        private readonly HashSet<string> _connectedAddresses = new HashSet<string>();

        public Dealer(string socketAddress, Action<string, string, IReceivingSocket> receiveReady)
            : this(Guid.NewGuid().ToString(), socketAddress, receiveReady)
        {
        }

        public Dealer(string identity, string socketAddress, Action<string, string, IReceivingSocket> receiveReady)
        {
            _receiveReady = receiveReady;
            _identity = identity;
            _socketAddress = socketAddress;
        }

        public void SendMessage(byte[] messageBytes)
        {
            if (_dealerSocket == null)
            {
                InitializeSocket();
            }

            Connect(_socketAddress);

            // The first frame must be empty,
            // followed by the message itself
            _dealerSocket.SendMoreFrameEmpty()
                .SendFrame(messageBytes);
        }

        public string Identity => _identity;

        public void Connect()
        {
            Connect(_socketAddress);
        }
        
        private void Connect(string socketAddress)
        {
            if (_connectedAddresses.Contains(socketAddress))
                return;

            _dealerSocket.Connect(socketAddress);
            _connectedAddresses.Add(socketAddress);
        }

        private void InitializeSocket()
        {
            if (_dealerSocket != null)
                return;

            _dealerSocket = new DealerSocket();
            _dealerSocket.Options.Identity = Encoding.UTF8.GetBytes(_identity);
            _dealerSocket.ReceiveReady += OnReceiveReady;

            _poller.Add(_dealerSocket);
            _poller.RunAsync();
        }

        private void OnReceiveReady(object sender, NetMQSocketEventArgs e)
        {
            var socket = e.Socket;

            // Discard the whole message if the first frame isn't empty
            var firstFrame = socket?.ReceiveFrameString();
            if (firstFrame?.Length != 0)
                return;
            
            // Pass the remaining message to the application
            _receiveReady(_identity, _socketAddress, socket);
        }

        public void Dispose()
        {
            _dealerSocket?.Dispose();
            _poller?.Dispose();
        }
    }
}