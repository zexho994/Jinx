import consumer.Consumer;
import org.junit.jupiter.api.Assertions;
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
    static String group_3 = "group_3";
    static String topic_4 = "topic_4";
    static String group_4 = "group_4";

    @Test
    public void consumer() {
        // step1: 启动生产者集群
        ProducerTest producerTest = new ProducerTest();
        producerTest.producer();

        // step2: 启动消费者集群
        // topic1 => 3个队列,3个消费组
        new Thread(() -> ConsumerTest.startConsumer(topic_1, group_1, 1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_1, group_1, 2)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_1, group_1, 3)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_1, group_2, 1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_1, group_2, 2)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_1, group_2, 3)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_1, group_3, 1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_1, group_3, 2)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_1, group_3, 3)).start();

        // topic2 => 2个队列，3个消费组
        new Thread(() -> ConsumerTest.startConsumer(topic_2, group_1, 1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_2, group_2, 1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_2, group_3, 1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_2, group_1, 2)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_2, group_2, 2)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_2, group_3, 2)).start();

        // topic3 => 1个队列,3个消费组
        new Thread(() -> ConsumerTest.startConsumer(topic_3, group_1, 1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_3, group_2, 1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_3, group_3, 1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_3, group_1, 1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_3, group_2, 1)).start();
        new Thread(() -> ConsumerTest.startConsumer(topic_3, group_3, 1)).start();

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
            Integer integer = ProducerTest.SEND_DATA_MAP.get(topic).get(queueId).get(msg.getTransactionId());
            if (integer == 3) {
                ProducerTest.SEND_DATA_MAP.get(topic).get(queueId).put(msg.getTransactionId(), 2);
            } else if (integer == 2) {
                ProducerTest.SEND_DATA_MAP.get(topic).get(queueId).put(msg.getTransactionId(), 1);
            } else {
                Assertions.assertEquals(ProducerTest.SEND_DATA_MAP.get(topic).get(queueId).get(msg.getTransactionId()), 1, "数值错误");
                Assertions.assertNotNull(ProducerTest.SEND_DATA_MAP.get(topic).get(queueId).remove(msg.getTransactionId()), "移除错误");
            }
        });
        consumer.start();
    }

}