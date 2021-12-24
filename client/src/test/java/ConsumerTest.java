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
        new Thread(() -> ConsumerTest.startConsumer(topic_1, group_1, 1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_1, group_1, 2)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_1, group_1, 3)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_1, group_1, 4)).start();
        // topic下不同group
        new Thread(() -> ConsumerTest.startConsumer(topic_2, group_1, 2)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_2, group_1, 5)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_2, group_2, 4)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_2, group_2, 6)).start();
        // group 订阅不同topic
        new Thread(() -> ConsumerTest.startConsumer(topic_3, group_1, 1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_3, group_1, 2)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_3, group_2, 1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_3, group_2, 2)).start();

        //阻塞
        while (true) {
        }

    }

    public static void startConsumer(String topic, String group, int queueId) {
        Consumer consumer = new Consumer("127.0.0.1");
        consumer.setTopic(topic);
        consumer.setConsumerGroup(group);
        consumer.setQueueId(queueId);

        // 消息监听
        consumer.setConsumerListener(msg -> {
            System.out.printf("[Consumer] topic = %s, group = %s, queue = %s, msg = %s \n", topic, group, queueId, msg);
            ProducerTest.SEND_DATA_MAP.get(topic).get(queueId).remove(msg.getTransactionId());
        });
        consumer.start();
    }

}