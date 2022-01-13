
import message.Message;
import org.junit.jupiter.api.Test;
import producer.DefaultMQProducer;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Zexho
 * @date 2021/11/16 8:59 上午
 */
public class DefaultMQProducerTest {

    /**
     * key = msgId
     * val = Map<queueId,Map<msgId,count>>
     */
    public static final Map<String, Map<Integer, Map<String, Integer>>> SEND_DATA_MAP = new ConcurrentHashMap<>(16);

    public static void produceMessage(String topic, int count, Map<String, Integer> set, int clusterSize) throws InterruptedException {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("127.0.0.1");
        defaultMQProducer.start();

        int s = 0;
        while (s < count) {
            s++;
            Message message = new Message();
            message.setTopic(topic);
            String msgId = UUID.randomUUID().toString();
            defaultMQProducer.sendMessage(message);
            set.put(msgId, clusterSize);

            Thread.sleep(500);
        }
    }

    @Test
    public void producer() {
        new Thread(() -> DefaultMQProducerTest.startProducer(6, "topic_1")).start();
        new Thread(() -> DefaultMQProducerTest.startProducer(8, "topic_1")).start();
        new Thread(() -> DefaultMQProducerTest.startProducer(7, "topic_2")).start();
        new Thread(() -> DefaultMQProducerTest.startProducer(9, "topic_2")).start();
        new Thread(() -> DefaultMQProducerTest.startProducer(10, "topic_3")).start();
        new Thread(() -> DefaultMQProducerTest.startProducer(12, "topic_3")).start();
    }

    @Test
    public void producerTest() {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("127.0.0.1");
        defaultMQProducer.start();
        Message message = new Message();
        message.setTopic("topic_1");
        defaultMQProducer.sendMessage(message);
    }

    /**
     * @param sleep 循环间隔（秒）
     * @param topic 消息主题
     */
    public static void startProducer(int sleep, String topic) {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("127.0.0.1");
        defaultMQProducer.start();

        int n = 1;
        sleep *= 1000;
        while (true) {
            Message message = new Message();
            message.setTopic(topic);
            message.setBody(++n);
            defaultMQProducer.sendMessage(message);

            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
