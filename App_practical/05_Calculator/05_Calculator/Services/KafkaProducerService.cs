using Confluent.Kafka;

namespace Calculator.Services
{
    public class KafkaProducerService<K, V>
    {
        private readonly IProducer<K, V> _producer;

        public KafkaProducerService(KafkaProducerHandler handler)
        {
            _producer = new DependentProducerBuilder<K, V>(
                handler.Handle).Build();
        }

        public Task ProduceAsync(string topic, Message<K, V> message)
        {
            return _producer.ProduceAsync(topic, message);
        }
    }
}