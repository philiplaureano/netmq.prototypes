using System;
using NetMQ;
using NetMQ.Sockets;

namespace PublisherNode
{
    class Program
    {
        static void Main(string[] args)
        {
            var publisherId = Guid.NewGuid();
            Console.WriteLine($"====== PUBLISHER {publisherId} ======");
            try
            {                
                using (var pubSocket = new PublisherSocket(">tcp://127.0.0.1:5309"))
                {
                    Console.WriteLine("Publisher socket connecting...");
                    pubSocket.Options.SendHighWatermark = 1000;

                    var rand = new Random(50);

                    while (true)
                    {
                        var randomizedTopic = rand.NextDouble();
                        if (randomizedTopic > 0.5)
                        {
                            var msg = "TopicA msg-" + randomizedTopic;
                            Console.WriteLine("Sending message : {0}", msg);
                            pubSocket.SendMoreFrame("TopicA").SendFrame(msg);
                        }
                        else
                        {
                            var msg = "TopicB msg-" + randomizedTopic;
                            Console.WriteLine("Sending message : {0}", msg);
                            pubSocket.SendMoreFrame("TopicB").SendFrame(msg);
                        }
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