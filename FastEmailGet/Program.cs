using System;
using System.Threading.Tasks;
using Amazon.SimpleEmail;
using Amazon.SimpleEmail.Model;
using Amazon.Util;

namespace FastEmailGet
{
    class FastEmailMailboxes
    {

        static void Main(string[] args)
        {
            var fe = new FastEmail(Amazon.RegionEndpoint.USEast1);
            foreach (var key in fe.Mailboxes.Keys)
            {
                Console.WriteLine("Recipient: {0}", key);
                Console.WriteLine($"   bucket: {fe.Mailboxes[key].S3Bucket}");
                Console.WriteLine($"   topic:  {fe.Mailboxes[key].TopicArn}");
            }

            fe.MonitorEmail("feed1@testaws.pcifarelli.net");
            string email = fe.WaitForNextEmail("feed1@testaws.pcifarelli.net", 1800);
            Console.WriteLine("Email:");
            Console.WriteLine($"{email}");
            fe.UnmonitorEmail("feed1@testaws.pcifarelli.net");
        }
    }
}
