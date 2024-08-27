
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace ConsumerApp.MessageBroker
{
    public class RabbitConsumerHostedService : IHostedService
    {
        private IConnection _connection;
        private IModel _channel;
        private string _exchange;
        private string _queueName;
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
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var consumer = new EventingBasicConsumer(_channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                // Process the message
                Console.WriteLine($"Received message: {message} Time: {DateTime.Now}");
            };
            _channel.BasicConsume(queue: _queueName, autoAck: true, consumer: consumer);

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
