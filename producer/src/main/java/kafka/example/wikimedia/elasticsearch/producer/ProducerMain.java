package kafka.example.wikimedia.elasticsearch.producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ProducerMain {
    private static final String URL = "https://stream.wikimedia.org/v2/stream/recentchange";

    public static void main(String[] args) throws InterruptedException, IOException {

        //Could be extract to the factory.
        Properties properties = new Properties();
        properties.load(KafkaEventBroker.class.getResourceAsStream("/kafka.properties"));
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        EventBroker broker = new KafkaEventBroker(kafkaProducer, "wikimedia.recentchange");

        EventHandler eventHandler = new WikiMediaChangeHandler(broker);

        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(URL));

        EventSource build = builder.build();
        build.start();
        TimeUnit.MINUTES.sleep(10);
    }

}
