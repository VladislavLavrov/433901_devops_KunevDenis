using Confluent.Kafka;

namespace Calculator.Services
{
    public class KafkaProducerHandler : IDisposable
    {
        private readonly IProducer<byte[], byte[]> _producer;

        public KafkaProducerHandler(IConfiguration config)
        {
            var settings = new ProducerConfig();
            config.GetSection("Kafka:ProducerSettings").Bind(settings);

            _producer = new ProducerBuilder<byte[], byte[]>(settings)
                .Build();
        }

        public Handle Handle => _producer.Handle;

        public void Dispose()
        {
            _producer.Flush();
            _producer.Dispose();
        }
    }
}