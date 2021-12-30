
import message.Message;
import org.junit.jupiter.api.Test;
import producer.Producer;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Zexho
 * @date 2021/11/16 8:59 上午
 */
public class ProducerTest {

    /**
     * key = msgId
     * val = Map<queueId,Map<msgId,count>>
     */
    public static final Map<String, Map<Integer, Map<String, Integer>>> SEND_DATA_MAP = new ConcurrentHashMap<>(16);

    @Test
    public void producer() {
        new Thread(() -> ProducerTest.startProducer(6, "topic_1")).start();
        new Thread(() -> ProducerTest.startProducer(8, "topic_1")).start();
        new Thread(() -> ProducerTest.startProducer(7, "topic_2")).start();
        new Thread(() -> ProducerTest.startProducer(9, "topic_2")).start();
        new Thread(() -> ProducerTest.startProducer(10, "topic_3")).start();
        new Thread(() -> ProducerTest.startProducer(12, "topic_3")).start();
    }

    @Test
    public void producerTest() {
        Producer producer = new Producer("127.0.0.1");
        producer.start();
        Message message = new Message();
        message.setTopic("topic_1");
        String msgId = UUID.randomUUID().toString();
        message.setTransactionId(msgId);
        producer.sendMessage(message);
    }

    /**
     * @param sleep 循环间隔（秒）
     * @param topic 消息主题
     */
    public static void startProducer(int sleep, String topic) {
        Producer producer = new Producer("127.0.0.1");
        producer.start();

        int n = 1;
        sleep *= 1000;
        while (true) {
            Message message = new Message();
            producer.setAfterRetryProcess(msg -> System.out.printf("== TOPIC = %s == \n", topic));
            message.setTopic(topic);
            String msgId = UUID.randomUUID().toString();
            message.setTransactionId(msgId);
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
