package kafka.example.wikimedia.elasticsearch.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Kafka implementation of {@link EventBroker}
 *
 * @author unal.asil
 */
public class KafkaEventBroker implements EventBroker {

    private final String topic;
    private final KafkaProducer<String, String> kafkaProducer;

    /**
     * Constructs a new {@link KafkaEventBroker} with given topic.
     *
     * @param topic topic
     */
    public KafkaEventBroker(KafkaProducer<String, String> kafkaProducer, String topic) {

        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void close() {
        kafkaProducer.close();
    }

    @Override
    public void publish(String message) {
        kafkaProducer.send(new ProducerRecord<>(topic, message));
    }
}
