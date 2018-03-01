using System.Net;

namespace Common.Messages
{
    public class ServiceAvailable
    {
        public ServiceAvailable(int serviceId, string hostName, int port)
        {
            ServiceId = serviceId;
            HostName = hostName;
            Port = port;
        }

        public int ServiceId { get; }
        public string HostName { get; }
        public int Port { get; }
    }
}