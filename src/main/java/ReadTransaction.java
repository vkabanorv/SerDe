import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.kstream.Consumed;


import java.io.BufferedReader;
import java.io.InputStreamReader;


public class ReadTransaction {

    public static void main(String[] args) throws Exception {
        //BufferedReader reader =
        //        new BufferedReader(new InputStreamReader(System.in));
        String bootstrapServers="127.0.0.1:9092";
        String groupId ="my-fifth-application";
        String topic="first_topic";
        String message = "{\"PLASTIC_ID\":96349208}";
        final Serde<Transaction> transactionSerde = MySerdes.serdeFrom(Transaction.class);
        ProducerDemo pd = new ProducerDemo(bootstrapServers,topic,message);
        ConsumerDemo cd = new ConsumerDemo(bootstrapServers,groupId,topic,transactionSerde.deserializer());


        while (true) {
            ConsumerRecords<String, Transaction> transactions = cd.Out();
            for (ConsumerRecord<String, Transaction> transaction : transactions) {
                System.out.println(transaction.value().getPlasticId());
            }
        }

    }
}