
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

    public static final Map<String, Integer> SEND_DATA = new ConcurrentHashMap<>(64);

    @Test
    public void producer() {
        new Thread(() -> ProducerTest.startProducer(10, "topic_1", 1)).start();
        new Thread(() -> ProducerTest.startProducer(12, "topic_1", 1)).start();
        new Thread(() -> ProducerTest.startProducer(15, "topic_2", 2)).start();
        new Thread(() -> ProducerTest.startProducer(18, "topic_2", 2)).start();
        new Thread(() -> ProducerTest.startProducer(15, "topic_3", 2)).start();
    }

    /**
     * @param sleep      循环间隔（秒）
     * @param topic      消息主题
     * @param groupCount 数量为订阅该 topic 的 consumeGroup 数量
     */
    public static void startProducer(int sleep, String topic, int groupCount) {
        Producer producer = new Producer("127.0.0.1");
        producer.start();

        int n = 0;
        sleep *= 1000;
        while (true) {
            Message message = new Message();
            producer.setAfterRetryProcess(msg -> System.out.printf("== TOPIC = %s == \n", topic));
            message.setTopic(topic);
            String msgId = UUID.randomUUID().toString();
            message.setTransactionId(msgId);
            message.setBody(++n);
            producer.sendMessage(message);
            SEND_DATA.put(msgId, groupCount);
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
