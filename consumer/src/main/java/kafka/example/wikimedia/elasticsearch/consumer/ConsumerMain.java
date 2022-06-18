package kafka.example.wikimedia.elasticsearch.consumer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerMain {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerMain.class);

    public static void main(String[] args) throws IOException {

        //TODO -> Make abstractions, create services for consumer and indexing.

        //Could be extract to the factory.
        Properties properties = new Properties();
        properties.load(ConsumerMain.class.getResourceAsStream("/kafka.properties"));
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchange"));

        Thread consumerHook = new Thread(kafkaConsumer::wakeup);
        Runtime.getRuntime().addShutdownHook(consumerHook);

        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 9200)).build();

        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        ElasticsearchClient client = new ElasticsearchClient(transport);

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                LOG.info("Total received message count: {}", records.count());
                for (ConsumerRecord<String, String> message : records) {
                    IndexRequest<IndexedMessage> indexMessageRequest =
                            new IndexRequest.Builder<IndexedMessage>().index("messages").id(message.key()).document(new IndexedMessage(message.value())).build();
                    LOG.info("{} is indexing", indexMessageRequest);
                    client.index(indexMessageRequest);
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
