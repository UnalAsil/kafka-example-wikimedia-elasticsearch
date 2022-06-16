package kafka.example.wikimedia.elasticsearch.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerMain {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerMain.class);

    public static void main(String[] args) throws IOException {

        //TODO -> Push messages to elastic
        //TODO -> Make broker abstractions.

        //Could be extract to the factory.
        Properties properties = new Properties();
        properties.load(ConsumerMain.class.getResourceAsStream("/kafka.properties"));
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchange"));

        Thread consumerHook = new Thread(kafkaConsumer::wakeup);
        Runtime.getRuntime().addShutdownHook(consumerHook);

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                LOG.info("Total received message count: {}", records.count());
                for (ConsumerRecord<String, String> message : records) {
                    LOG.info("Incoming message : {}", message.value());
                }
            }
        } catch (WakeupException e) {
            LOG.error("Kafka wake up exception");
        } catch (Exception e) {
            LOG.error("Some error occured", e);
        } finally {
            kafkaConsumer.close();
        }

    }

}
