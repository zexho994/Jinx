import consumer.Consumer;
import org.junit.jupiter.api.Test;

/**
 * @author Zexho
 * @date 2021/11/16 8:56 上午
 */
public class ConsumerTest {

    static String topic_1 = "topic_1";
    static String group_1 = "group_1";
    static String topic_2 = "topic_2";
    static String group_2 = "group_2";
    static String topic_3 = "topic_3";

    @Test
    public void consumer() {
        // 同一集群，同一topic
        new Thread(() -> ConsumerTest.startConsumer(topic_1, group_1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_1, group_1)).start();

        // topic下不同group
        new Thread(() -> ConsumerTest.startConsumer(topic_1, group_2)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_1, group_2)).start();

        // group 订阅不同topic
        new Thread(() -> ConsumerTest.startConsumer(topic_2, group_1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_3, group_2)).start();
        while (true) {
        }

    }

    public static void startConsumer(String topic, String group) {
        Consumer consumer = new Consumer("127.0.0.1");
        consumer.setTopic(topic);
        consumer.setConsumerGroup(group);
        consumer.setConsumerListener(msg -> {
            System.out.printf("consumer, time = %s, msg = %s \n", System.currentTimeMillis(), msg);
        });
        consumer.start();
    }

}