import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.kafka.streams.kstream.Consumed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    String bootstrapServers;
    String groupId;
    String topic;
    Logger logger;
    Properties properties;
    KafkaConsumer<String, Transaction> consumer;

    public ConsumerDemo(String bootstrapServers, String groupId, String topic,Deserializer<Transaction> deserializerTransaction) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.topic = topic;
        this.logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        // create consumer configs
        this.properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializerTransaction.getClass().getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializerTransaction.getClass().getName());
        //properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create a consumer
        this.consumer = new KafkaConsumer<>(properties);

        // subscribe consumer to our topic(s)
        consumer.subscribe(Collections.singleton(topic));
    }

    public ConsumerRecords<String, Transaction> Out() {
        return consumer.poll(1000l);
        // poll for new data
    }

}
