import entity.Transaction;
import kafka.MessageReader;
import kafka.MessageSender;

import java.util.List;

public class SerializationTest {
    public static void main(String[] args) {
        String topic = "first_topic";
        MessageSender sender = new MessageSender(topic);
        MessageReader reader = new MessageReader(topic);

        sender.send("{\"PLASTIC_ID\":96349207}");
        sender.send("{\"PLASTIC_ID\":96349208}");
        sender.send("{\"PLASTIC_ID\":96349209}");
        sender.flush();
        sender.close();

        List<Transaction> transactions = reader.pollValues(10000);
        transactions.forEach(x -> System.out.println(x.getPlasticId()));
    }
}
