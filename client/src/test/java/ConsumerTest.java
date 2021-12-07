import consumer.Consumer;

/**
 * @author Zexho
 * @date 2021/11/16 8:56 上午
 */
public class ConsumerTest {

    static String topic = "Topic_MessageOrderingTest";
    static String group = "Group_MessageOrderingTest";

    public static void main(String[] args) {
        ConsumerTest consumerTest = new ConsumerTest();
        new Thread(() -> consumerTest.startConsumer1(topic, group)).start();
    }

    public void startConsumer1(String topic, String group) {
        System.out.println("start consumer 1");
        Consumer consumer = new Consumer("127.0.0.1");
        consumer.setTopic(topic);
        consumer.setConsumerGroup(group);
        consumer.setConsumerListener(msg -> {
            System.out.printf("consumer, time = %s, msg = %s \n", System.currentTimeMillis(), msg);
        });
        consumer.start();
    }

    public void startConsumer2() {
        System.out.println("start consumer 2");
        Consumer consumer = new Consumer("127.0.0.1");
        consumer.setConsumerListener(msg -> {
            System.out.printf("consumer 2, time = %s, msg = %s \n", System.currentTimeMillis(), msg);
        });
        consumer.setTopic("topic_2");
        consumer.setConsumerGroup("consumer_group_2");
        consumer.start();
    }

}