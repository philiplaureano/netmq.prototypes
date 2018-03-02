using System;
using NetMQ;
using NetMQ.Sockets;

namespace SubscriberNode
{
    class Program
    {
        static void Main(string[] args)
        {
            var topic = "TopicA";
            var subscriberId = Guid.NewGuid();
            Console.WriteLine($"====== SUBSCRIBER {subscriberId} ======");
            
            try
            {
                using (var subSocket = new SubscriberSocket(">tcp://127.0.0.1:8675"))
                {
                    subSocket.Options.ReceiveHighWatermark = 1000;
                    subSocket.Subscribe(topic);
                    
                    Console.WriteLine("Subscriber socket connecting...");

                    while (true)
                    {
                        var currentTopic = subSocket.ReceiveFrameString();
                        var messageReceived = subSocket.ReceiveFrameString();
                        
                        Console.WriteLine($"[Subscriber {subscriberId}, Topic '{currentTopic}']: {messageReceived}");
                    } 
                }
            }
            finally
            {
                NetMQConfig.Cleanup();
            }
        }
    }
}