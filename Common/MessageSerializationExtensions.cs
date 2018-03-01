using System;
using System.IO;
using NetMQ;
using NetMQ.Sockets;
using NFX.Serialization.Slim;

namespace Common
{
    public static class NetMQExtensions
    {
        public static void Publish(this object message, string topic, string publisherAddress)
        {
            using (var pubSocket = new PublisherSocket(publisherAddress))
            {
                var bytes = message.GetBytes();
                var frame = pubSocket.SendMoreFrame(topic);
                frame.SendFrame(bytes);

                pubSocket.Close();
            }
        }
    }

    public static class MessageSerializationExtensions
    {
        public static byte[] GetBytes(this object item)
        {
            var serializer = new SlimSerializer();
            using (var stream = new MemoryStream())
            {
                serializer.Serialize(stream, item);
                return stream.ToArray();
            }
        }

        public static T Deserialize<T>(this byte[] bytes)
            where T : class
        {
            var serializer = new SlimSerializer();
            using (var stream = new MemoryStream(bytes))
            {
                return serializer.Deserialize(stream) as T;
            }
        }
    }
}