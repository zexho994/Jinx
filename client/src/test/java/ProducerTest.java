
import Message.Message;
import producer.Producer;

import java.util.UUID;

/**
 * @author Zexho
 * @date 2021/11/16 8:59 上午
 */
public class ProducerTest {

    public static void main(String[] args) {
        new Thread(() -> ProducerTest.startProducer1(1000)).start();
        new Thread(() -> ProducerTest.startProducer2(1000)).start();
    }

    public static void startProducer1(int sleep) {
        Producer producer = new Producer("group_1", "127.0.0.1");
        producer.start();

        Message message1 = new Message();
        message1.setTopic("topic_1");
        int n = 0;
        while (true) {
            message1.setTransactionId(UUID.randomUUID().toString());
            message1.setBody(++n);
            producer.sendMessage(message1);
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void startProducer2(int sleep) {
        Producer producer = new Producer("group_2", "127.0.0.1");
        producer.start();

        Message message2 = new Message();
        message2.setTopic("topic_2");
        int n = 0;
        while (true) {
            message2.setTransactionId(UUID.randomUUID().toString());
            message2.setBody(++n);
            producer.sendMessage(message2);
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
