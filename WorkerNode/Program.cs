using System;
using System.IO;
using Common.Messages;
using NetMQ;
using NetMQ.Sockets;
using NFX.IO.Net.Gate;
using NFX.Serialization.Slim;

namespace WorkerNode
{
    // Prototype #1: A consumer that polls job requests from a master node
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                var workerId = Guid.NewGuid();
                Console.WriteLine($"====== WORKER {workerId} ======");

                using (var sender = new PushSocket(">tcp://localhost:5558"))
                using (var receiver = new PullSocket(">tcp://localhost:5557"))
                {
                    var serializer = new SlimSerializer();
                    while (true)
                    {
                        var bytes = receiver.ReceiveFrameBytes();
                        if (bytes.Length == 0)
                            continue;

                        if (!(serializer.Deserialize(new MemoryStream(bytes)) is CalculateFibonacci message))
                            continue;

                        var number = message.Number;
                        var result = GetFibonacci(number);

                        var resultMessage = new CalculationResult(number, result, workerId);
                        var resultStream = new MemoryStream();
                        serializer.Serialize(resultStream, resultMessage);
                    
                        sender.SendFrame(resultStream.ToArray());
                    }
                }
            }
            finally
            {
                NetMQConfig.Cleanup();
            }            
        }

        private static int GetFibonacci(int number)
        {
            if (number <= 0)
                return 0;

            return number + GetFibonacci(number - 1);
        }
    }
}