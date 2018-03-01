using System;
using System.IO;
using System.Threading.Tasks;
using Common.Messages;
using NetMQ;
using NetMQ.Sockets;
using NFX.Serialization.Slim;

namespace WorkerSinkNode
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                Console.WriteLine("====== SINK ======");
                Task.Run(() =>
                {
                    using (var receiver = new PullSocket("@tcp://localhost:5558"))
                    {
                        var serializer = new SlimSerializer();
                        while (true)
                        {
                            var bytes = receiver.ReceiveFrameBytes();
                            if (serializer.Deserialize(new MemoryStream(bytes)) 
                                is CalculationResult result)
                            {
                                var workerId = result.WorkerId;
                                Console.WriteLine($"Worker {workerId}: The fibonacci of '{result.Number}' is '{result.Result}'");
                            }                        
                        }
                    }
                });

                Console.WriteLine("Press ENTER to terminate the program");
                Console.ReadLine();
            }
            finally
            {
                NetMQConfig.Cleanup();
            }            
        }
    }
}