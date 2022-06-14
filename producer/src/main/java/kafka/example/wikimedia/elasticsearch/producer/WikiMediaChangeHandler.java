package kafka.example.wikimedia.elasticsearch.producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handles the wikimedia stream. It publishes the data to the broker.
 *
 * @author unal.asil
 */
public class WikiMediaChangeHandler implements EventHandler {

    private static final Logger LOG = LoggerFactory.getLogger(WikiMediaChangeHandler.class);
    private final EventBroker eventBroker;

    /**
     * Constructs a new {@link WikiMediaChangeHandler} with given event broker.
     *
     * @param eventBroker Publish the data.
     */
    public WikiMediaChangeHandler(EventBroker eventBroker) {
        this.eventBroker = eventBroker;
    }

    @Override
    public void onOpen() {
        //Do nothing.
    }

    @Override
    public void onClosed() {
        eventBroker.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) {
        eventBroker.publish(messageEvent.getData());
    }

    @Override
    public void onComment(String s) {
        //Do nothing.
    }

    @Override
    public void onError(Throwable throwable) {
        LOG.error("Some error in stream reading", throwable);
    }
}
