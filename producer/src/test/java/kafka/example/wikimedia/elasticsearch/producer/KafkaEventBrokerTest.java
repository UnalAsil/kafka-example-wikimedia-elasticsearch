package kafka.example.wikimedia.elasticsearch.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Test class for {@link KafkaEventBroker}
 *
 * @author unal.asil
 */
class KafkaEventBrokerTest {

    @Test
    void whenKafkaEventBrokerClose_thenKafkaProducerClose() {
        //given
        KafkaProducer<String, String> kafkaProducer = mock(KafkaProducer.class);
        String anyTopic = "anyTopic";
        KafkaEventBroker kafkaEventBroker = new KafkaEventBroker(kafkaProducer, anyTopic);

        //when
        kafkaEventBroker.close();

        //then
        verify(kafkaProducer, only()).close();

    }

    @Test
    void whenKafkaEventBrokerPublishMessage_thenKafkaProducerPublishMessageWithSameValueAndInitialTopic() {
        //given
        KafkaProducer<String, String> kafkaProducer = mock(KafkaProducer.class);
        String anyTopic = "anyTopic";
        KafkaEventBroker kafkaEventBroker = new KafkaEventBroker(kafkaProducer, anyTopic);

        ArgumentCaptor<ProducerRecord> recordArgumentCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        //when
        String anyMessage = "anyMessage";
        kafkaEventBroker.publish(anyMessage);

        //then
        verify(kafkaProducer, only()).send(recordArgumentCaptor.capture());
        ProducerRecord capturedRecord = recordArgumentCaptor.getValue();
        assertEquals(anyTopic, capturedRecord.topic());
        assertEquals(anyMessage, capturedRecord.value());

    }
}