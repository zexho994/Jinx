import consumer.Consumer;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * @author Zexho
 * @date 2021/11/16 8:56 上午
 */
public class ConsumerTest {

    @Test
    public void start() {
        startConsumer("topic_1", "group_1", 1, null);
    }

    public static void startConsumer(String topic, String group, int cid, Map<String, Integer> msgMap) {
        Consumer consumer = new Consumer("127.0.0.1", cid);
        consumer.setTopic(topic);
        consumer.setConsumerGroup(group);

        // 消息监听
        consumer.setConsumerListener(msg -> {
            System.out.printf("[Consumer] cid =%s topic = %s, group = %s, msg = %s \n", cid, topic, group, msg);
        });
        consumer.start();
    }

}