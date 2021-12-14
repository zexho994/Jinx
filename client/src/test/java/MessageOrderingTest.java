import message.Message;
import consumer.Consumer;
import org.junit.jupiter.api.Assertions;
import producer.Producer;

import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Zexho
 * @date 2021/12/6 8:11 下午
 */
public class MessageOrderingTest {

    Queue<String> sendQueue = new LinkedBlockingQueue<>();

    public static void main(String[] args) {
        String topic = "Topic_MessageOrderingTest";
        String group = "Group_MessageOrderingTest";

        MessageOrderingTest test = new MessageOrderingTest();
        new Thread(() -> test.send(topic)).start();
        new Thread(() -> test.consume(topic, group)).start();
    }

    public void send(String topic) {
        Producer producer = new Producer( "127.0.0.1");
        producer.start();

        Message message = new Message();
        message.setTopic(topic);
        int n = 0;
        while (true) {
            String msgId = UUID.randomUUID().toString();
            message.setTransactionId(msgId);
            message.setBody(++n);
            producer.sendMessage(message);
            this.sendQueue.offer(msgId);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void consume(String topic, String group) {
        Consumer consumer = new Consumer("127.0.0.1");
        AtomicInteger n = new AtomicInteger(1);
        consumer.setConsumerListener(msg -> {
            System.out.printf("\n[CONSUME] Receiver message => %s \n\n", msg);
            int body = (int) msg.getBody();
            Assertions.assertEquals(body, (n.getAndIncrement()));
        });
        consumer.setTopic(topic);
        consumer.setConsumerGroup(group);
        consumer.start();
    }

}
