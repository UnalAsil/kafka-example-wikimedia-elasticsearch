package kafka.example.wikimedia.elasticsearch.consumer;

public class IndexedMessage {

    private final String value;

    public IndexedMessage(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
