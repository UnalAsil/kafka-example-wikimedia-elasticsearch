package kafka.example.wikimedia.elasticsearch.producer;

import com.launchdarkly.eventsource.MessageEvent;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

/**
 * Test class for {@link WikiMediaChangeHandler}
 *
 * @author unal.asil
 */
class WikiMediaChangeHandlerTest {

    @Test
    void whenOnClosedEvent_thenCloseEventBroker() {
        //given
        EventBroker eventBroker = mock(EventBroker.class);
        WikiMediaChangeHandler wikiMediaChangeHandler = new WikiMediaChangeHandler(eventBroker);

        //when
        wikiMediaChangeHandler.onClosed();

        //then
        verify(eventBroker, only()).close();
    }

    @Test
    void whenOnMessageEvent_thenPublishToTheBroker() {
        //given
        EventBroker eventBroker = mock(EventBroker.class);
        WikiMediaChangeHandler wikiMediaChangeHandler = new WikiMediaChangeHandler(eventBroker);

        MessageEvent messageEvent = mock(MessageEvent.class);

        //when
        wikiMediaChangeHandler.onMessage("any", messageEvent);

        //then
        verify(eventBroker, only()).publish(messageEvent.getData());
    }
}