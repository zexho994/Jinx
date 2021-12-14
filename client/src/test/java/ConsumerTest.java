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
        // step1: 启动生产者集群
        ProducerTest producerTest = new ProducerTest();
        producerTest.producer();

        // step2: 启动消费者集群
        // 同一集群，同一topic
        new Thread(() -> ConsumerTest.startConsumer(topic_1, group_1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_1, group_1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_1, group_1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_1, group_1)).start();
        // topic下不同group
        new Thread(() -> ConsumerTest.startConsumer(topic_2, group_1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_2, group_1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_2, group_2)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_2, group_2)).start();
        // group 订阅不同topic
        new Thread(() -> ConsumerTest.startConsumer(topic_3, group_1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_3, group_1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_3, group_2)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_3, group_2)).start();

        //阻塞
        while (true) {
        }

    }

    public static void startConsumer(String topic, String group) {
        Consumer consumer = new Consumer("127.0.0.1");
        consumer.setTopic(topic);
        consumer.setConsumerGroup(group);

        // 消息监听
        consumer.setConsumerListener(msg -> {
            System.out.printf("消息消息 => topic = %s, group = %s, msg = %s \n", topic, group, msg);
            Integer n = ProducerTest.SEND_DATA.get(msg.getTransactionId());
            if (n == 1) {
                ProducerTest.SEND_DATA.remove(msg.getTransactionId());
            } else {
                ProducerTest.SEND_DATA.put(msg.getTransactionId(), n - 1);
            }
        });
        consumer.start();
    }

}