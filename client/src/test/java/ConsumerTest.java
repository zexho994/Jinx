import consumer.Consumer;
import org.junit.jupiter.api.Test;

import java.util.Set;

/**
 * @author Zexho
 * @date 2021/11/16 8:56 上午
 */
public class ConsumerTest {

    public static void startConsumer(String topic, String group, int cid, Set<String> msgSet) {
        Consumer consumer = new Consumer("127.0.0.1", cid);
        consumer.setTopic(topic);
        consumer.setConsumerGroup(group);

        // 消息监听
        consumer.setConsumerListener(msg -> {
            System.out.printf("[Consumer] cid =%s topic = %s, group = %s, msg = %s \n", cid, topic, group, msg);
            msgSet.remove(msg.getTransactionId());
        });
        consumer.start();
    }

    @Test
    public void registerTest() {
//        Consumer consumer = new Consumer("127.0.0.1", 1);
//        consumer.setConsumerGroup("register_group_1");
//        consumer.setTopic("topic_1");
//        consumer.setConsumerListener(System.out::println);
//        consumer.start();
//
//
//        Consumer consumer2 = new Consumer("127.0.0.1", 2);
//        consumer2.setConsumerGroup("register_group_2");
//        consumer2.setTopic("topic_1");
//        consumer2.setConsumerListener(System.out::println);
//        consumer2.start();

        Consumer consumer3 = new Consumer("127.0.0.1", 3);
        consumer3.setConsumerGroup("register_group_3");
        consumer3.setTopic("topic_1");
        consumer3.setConsumerListener(message -> System.out.printf("[Consumer] cid = 3, message = %s \n", message));
        consumer3.start();

        Consumer consumer4 = new Consumer("127.0.0.1", 4);
        consumer4.setConsumerGroup("register_group_3");
        consumer4.setTopic("topic_1");
        consumer4.setConsumerListener(message -> System.out.printf("[Consumer] cid = 4, message = %s \n", message));
        consumer4.start();
        while (true) {
        }
    }

}