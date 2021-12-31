import consumer.Consumer;

import java.util.Map;

/**
 * @author Zexho
 * @date 2021/11/16 8:56 上午
 */
public class ConsumerTest {

    public static void startConsumer(String topic, String group, int cid, Map<String, Integer> msgMap) {
        Consumer consumer = new Consumer("127.0.0.1", cid);
        consumer.setTopic(topic);
        consumer.setConsumerGroup(group);

        // 消息监听
        consumer.setConsumerListener(msg -> {
            System.out.printf("[Consumer] cid =%s topic = %s, group = %s, msg = %s \n", cid, topic, group, msg);
            Integer size = msgMap.get(msg.getTransactionId());
            if (size == 1) {
                msgMap.remove(msg.getTransactionId());
            } else {
                msgMap.put(msg.getTransactionId(), size - 1);
            }
        });
        consumer.start();
    }

}