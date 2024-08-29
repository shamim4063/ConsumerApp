using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using Microsoft.Extensions.Hosting;
using System.Collections.Concurrent;
using Polly;
using Polly.RateLimit;
using System.Text.Json;

namespace ConsumerApp.MessageBroker
{
    public class RabbitConsumerHostedService : IHostedService
    {
        private IConnection _connection;
        private IModel _channel;
        private string _exchange;
        private string _queueName;

        // In-memory storage to track the last message per key
        private static readonly ConcurrentDictionary<string, string> LatestMessages = new ConcurrentDictionary<string, string>();

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
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);

                // Extract the unique identifier from the message (e.g., LeadRefNo)
                var messageId = ExtractMessageId(message);

                // Store or update the latest message for the given key
                LatestMessages.AddOrUpdate(messageId, message, (key, oldValue) => message);

                // Delay processing to simulate work and allow other messages to arrive
                await Task.Delay(200);

                // Now, check if the current message is still the latest one
                if (LatestMessages.TryGetValue(messageId, out var latestMessage) && latestMessage == message)
                {
                    try
                    {
                        // Execute the message processing within the rate limit policy
                        _rateLimitPolicy.Execute(() =>
                        {
                            // Since Polly's RateLimitPolicy is synchronous, use Task.Run for async work
                            Task.Run(async () =>
                            {
                                // Process the latest message
                                Console.WriteLine($"Processing latest message: {message} for key: {messageId} Time: {DateTime.Now}");

                                // Simulate processing delay
                                await Task.Delay(200); // Simulate work

                                // Acknowledge the message
                                _channel.BasicAck(ea.DeliveryTag, false);
                            }).Wait();
                        });
                    }
                    catch (RateLimitRejectedException)
                    {
                        Console.WriteLine($"Taking rest because of over message");
                        // If the rate limit is hit, delay and retry
                        await Task.Delay(200);
                    }

                    // Remove the processed message from the tracking dictionary
                    LatestMessages.TryRemove(messageId, out _);
                }
                else
                {
                    // The current message is outdated, acknowledge and discard
                    Console.WriteLine($"Discarding outdated message: {message} for key: {messageId}");
                    _channel.BasicAck(ea.DeliveryTag, false);
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

        private string ExtractMessageId(string message)
        {
            var messageData = JsonSerializer.Deserialize<MessageDto>(message);
            if (messageData == null)
                throw new Exception("Invalid Message");
            return messageData.Feature + "-" + messageData.LeadGuId;
        }
    }

    public class MessageDto
    {
        public string LeadGuId { get; set; }
        public string Feature { get; set; }
        public Dictionary<string, string> PropertyKeyValues { get;set;}
    }
}
