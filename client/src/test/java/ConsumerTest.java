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

        //阻塞
        while (true) {
        }

    }

    public static void startConsumer(String topic, String group, int cid) {
        Consumer consumer = new Consumer("127.0.0.1", cid);
        consumer.setTopic(topic);
        consumer.setConsumerGroup(group);

        // 消息监听
        consumer.setConsumerListener(msg -> {
            System.out.printf("[Consumer] cid =%s topic = %s, group = %s, msg = %s \n", cid, topic, group, msg);
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
//        Consumer consumer4 = new Consumer("127.0.0.1", 4);
//        consumer4.setConsumerGroup("register_group_3");
//        consumer4.setTopic("topic_1");
//        consumer4.setConsumerListener(message -> System.out.printf("[Consumer] cid = 4, message = %s \n", message));
//        consumer4.start();
        while (true) {
        }
//        Consumer consumer5 = new Consumer("127.0.0.1", 5);
//        consumer5.setConsumerGroup("register_group_3");
//        consumer5.setTopic("topic_1");
//        consumer5.setConsumerListener(System.out::println);
//        consumer5.start();
    }


}