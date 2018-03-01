using System;
using System.IO;
using Common.Messages;
using NetMQ;
using NetMQ.Sockets;
using NFX.Serialization.Slim;

namespace WorkDispatcherNode
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                using (var sender = new PushSocket("@tcp://*:5557"))
                {
                    while (true)
                    {
                        Console.WriteLine("Press enter when the workers are ready");
                        Console.ReadLine();
                
                        Console.WriteLine("Sending tasks to workers");

                        var maxValue = 1000;
                        var rand = new Random();
                        for (var i = 0; i < 100; i++)
                        {
                            var message = new CalculateFibonacci(rand.Next(maxValue));

                            // Convert the message into a byte stream
                            var stream = new MemoryStream();
                            var serializer = new SlimSerializer();
                            serializer.Serialize(stream, message);

                            // Send the message off to the worker nodes
                            var bytes = stream.ToArray();
                            sender.SendFrame(bytes);
                        }
                
                        Console.WriteLine("Press Enter to quit, or press any other key to repeat the run");
                        var key = Console.ReadKey();

                        if (key.Key == ConsoleKey.Enter)
                            break;    
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