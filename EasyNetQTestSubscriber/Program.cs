using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ENQMessageLib;
using EasyNetQ;
using EasyNetQ.SystemMessages;
using EasyNetQ.Topology;


namespace EasyNetQTestSubscriber
{
    class Program
    {
        static void Main(string[] args)
        {
            var bus = RabbitHutch.CreateBus("host=localhost").Advanced;

            var queue = bus.QueueDeclare("emailsubscriptionAdvanced");

            bus.Consume<ENQMessage>(queue,
                                    (message, info) =>
                                    Task.Factory.StartNew(() =>
                                        {
                                            throw new Exception("oops!!");
                                            //Console.WriteLine(message.Body.EmailAddress);
                                        }));
            
            var errorQueueName = new Conventions().ErrorQueueNamingConvention();
            var errqueue = bus.QueueDeclare(errorQueueName);





            //ERRORS
            bus.Consume<Error>(errqueue, (message, info) =>
            {
                var error = message.Body;

                Console.WriteLine("error.DateTime = {0}", error.DateTime);
                Console.WriteLine("error.Exception = {0}", error.Exception);
                Console.WriteLine("error.Message = {0}", error.Message);
                Console.WriteLine("error.RoutingKey = {0}", error.RoutingKey);


                return Task.Factory.StartNew(() => { });
            });



            Console.ReadLine();
            bus.Dispose();
        }
    }
}
