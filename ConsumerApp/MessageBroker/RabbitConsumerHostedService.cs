using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using Polly;
using Polly.RateLimit;
using Microsoft.Extensions.Hosting;

namespace ConsumerApp.MessageBroker
{
    public class RabbitConsumerHostedService : IHostedService
    {
        private IConnection _connection;
        private IModel _channel;
        private string _exchange;
        private string _queueName;

        // Polly Rate Limiting Policy
        private readonly RateLimitPolicy _rateLimitPolicy;

        public RabbitConsumerHostedService()
        {
            var factory = new ConnectionFactory
            {
                HostName = "95.45.224.19",
                Port = 5672,
                VirtualHost = "qsmartdev",
                UserName = "essadmin",
                Password = "123@qwe"
            };

            _connection = factory.CreateConnection();
            _channel = _connection.CreateModel();
            _queueName = "Queue1";
            _exchange = "RnDExchange";

            _channel.ExchangeDeclare(exchange: _exchange, type: ExchangeType.Fanout, durable: true);
            _channel.QueueDeclare(queue: _queueName, durable: true, exclusive: false, autoDelete: false);
            _channel.QueueBind(queue: _queueName, exchange: _exchange, routingKey: _queueName);

            // Initialize the rate limiting policy: 5 executions per second
            _rateLimitPolicy = Policy.RateLimit(5, TimeSpan.FromSeconds(1));
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var consumer = new EventingBasicConsumer(_channel);
            consumer.Received += async (model, ea) =>
            {
                while (true)
                {
                    try
                    {
                        // Execute the message processing within the rate limit policy
                        _ = _rateLimitPolicy.Execute(() =>
                        {
                            // Since Polly's RateLimitPolicy is synchronous, use Task.Run for async work
                            return Task.Run(async () =>
                            {
                                var body = ea.Body.ToArray();
                                var message = Encoding.UTF8.GetString(body);
                                // Process the message
                                Console.WriteLine($"Received message: {message} Time: {DateTime.Now}");

                                // Simulate processing delay
                                await Task.Delay(200); // Simulate work

                                // Acknowledge the message
                                _channel.BasicAck(ea.DeliveryTag, false);
                            });
                        });

                        break; // Break out of the loop once the message is successfully processed
                    }
                    catch (RateLimitRejectedException)
                    {
                        // If the rate limit is hit, wait and retry
                        Console.WriteLine("Taking rest before next messages");
                        await Task.Delay(200); // Wait before retrying, adjust the delay as needed
                    }
                }
            };

            _channel.BasicConsume(queue: _queueName, autoAck: false, consumer: consumer);

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _channel.Close();
            _connection.Close();
            return Task.CompletedTask;
        }
    }
}
