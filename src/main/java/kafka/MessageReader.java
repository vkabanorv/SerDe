package kafka;

import entity.Transaction;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import serdes.TransactionDeserializer;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MessageReader {
    private Consumer<String,Transaction> consumer;
    private String topic;

    public MessageReader(String topic) {
        this.topic = topic;
        this.consumer = KafkaObjectFactory.getConsumer();
        this.consumer.subscribe(Collections.singleton(topic));
    }

    public ConsumerRecords<String,Transaction> poll (long duration) {
        return this.consumer.poll(duration);
    }

    public List<Transaction> pollValues (long duration) {
        Iterator<ConsumerRecord<String,Transaction>> iterator = this.consumer.poll(duration).iterator();
        Iterable<ConsumerRecord<String,Transaction>> iterable = () -> iterator;
        List<ConsumerRecord<String,Transaction>> records = StreamSupport
                .stream(iterable.spliterator(), false)
                .collect(Collectors.toList());
        List<Transaction> transactions = records.stream()
                .map(x -> x.value())
                .collect(Collectors.toList());
        return transactions;
    }

    public String getTopic() {
        return topic;
    }
}
