package kafka.example.wikimedia.elasticsearch.producer;

/**
 * Publish message to the brokers.
 *
 * @author unal.asil
 */
public interface EventBroker {

    /**
     * Close the broker.
     */
    void close();

    /**
     * Publish message to the brokers.
     *
     * @param message published message.
     */
    void publish(String message);
}
