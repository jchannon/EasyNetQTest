using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ENQMessageLib;
using EasyNetQ;
using EasyNetQ.Topology;

namespace ConsoleApplication1
{
    class Program
    {
        static void Main(string[] args)
        {
            var bus = RabbitHutch.CreateBus("host=localhost").Advanced;

            var queue = bus.QueueDeclare("emailsubscriptionAdvanced");

            bus.Bind(bus.ExchangeDeclare("myAdvancedExchange", ExchangeType.Topic), queue, "#");

            for (int i = 0; i < 10; i++)
            {

                using (var publishChannel = bus.OpenPublishChannel())
                {
                    var message = new Message<ENQMessage>(new ENQMessage()
                    {
                        Name = i + "bill",
                        LastName = i + "baggins",
                        EmailAddress = i + "bill@home.com"
                    });

                    var exch = bus.ExchangeDeclare("myAdvancedExchange", ExchangeType.Topic);
                    publishChannel.Publish<ENQMessage>(exch, "#", message);
                }
            }

            bus.Dispose();
        }
    }
}
