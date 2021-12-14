
import message.Message;
import org.junit.jupiter.api.Test;
import producer.Producer;

import java.util.UUID;

/**
 * @author Zexho
 * @date 2021/11/16 8:59 上午
 */
public class ProducerTest {

    @Test
    public void producer() throws InterruptedException {
        new Thread(() -> ProducerTest.startProducer(10, "topic_1")).start();
        new Thread(() -> ProducerTest.startProducer(12, "topic_1")).start();
        new Thread(() -> ProducerTest.startProducer(15, "topic_2")).start();
        new Thread(() -> ProducerTest.startProducer(18, "topic_3")).start();
        Thread topic_3 = new Thread(() -> ProducerTest.startProducer(15, "topic_3"));
        topic_3.start();
        topic_3.join();
    }

    public static void startProducer(int sleep, String topic) {
        Producer producer = new Producer("127.0.0.1");
        producer.start();

        int n = 0;
        sleep *= 1000;
        while (true) {
            Message message = new Message();
            producer.setAfterRetryProcess(msg -> System.out.printf("== TOPIC = %s == \n", topic));
            message.setTopic(topic);
            message.setTransactionId(UUID.randomUUID().toString());
            message.setBody(++n);
            producer.sendMessage(message);
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
