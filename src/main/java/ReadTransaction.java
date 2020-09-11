import entity.Transaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.*;

import java.util.HashMap;
import java.util.Map;

import serdes.JsonPOJODeserializer;
import serdes.MySerdes;
import serdes.TransactionDeserializer;


public class ReadTransaction {

    public static void main(String[] args) throws Exception {


        //BufferedReader reader =
        //        new BufferedReader(new InputStreamReader(System.in));
        String bootstrapServers="localhost:9092";
        String groupId ="my-fifth-application";
        String topic="first_topic";
        String message = "{\"PLASTIC_ID\":96349208}";
        final Serde<Transaction> transactionSerde = MySerdes.serdeFrom(Transaction.class);
        ProducerDemo pd = new ProducerDemo(bootstrapServers,topic,message);
        ConsumerDemo cd = new ConsumerDemo(bootstrapServers,groupId,topic,transactionSerde.deserializer());

        while (true) {
            ConsumerRecords<String, Transaction> transactions = cd.consumer.poll(1000);
            for (ConsumerRecord<String, Transaction> transaction : transactions) {
                System.out.println(transaction.value().getPlasticId());
            }
        }

    }
}