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

            var exchange = bus.ExchangeDeclare("myAdvancedExchange", ExchangeType.Direct);

            bus.Bind(exchange, queue, "#");

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

                    publishChannel.Publish<ENQMessage>(exchange, "#", message);
                }
            }

            bus.Dispose();
        }
    }
}
