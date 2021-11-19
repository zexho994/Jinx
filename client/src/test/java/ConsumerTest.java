import consumer.Consumer;

/**
 * @author Zexho
 * @date 2021/11/16 8:56 上午
 */
public class ConsumerTest {
    public static void main(String[] args) {
        ConsumerTest consumerTest = new ConsumerTest();
        new Thread(consumerTest::startConsumer1).start();
        new Thread(consumerTest::startConsumer2).start();
    }

    public void startConsumer1() {
        System.out.println("start consumer 1");
        Consumer consumer = new Consumer("127.0.0.1");
        consumer.setConsumerListener(msg -> System.out.println("consumer 1 "));
        consumer.setTopic("topic_1");
        consumer.setConsumerGroup("consumer_group_1");
        consumer.start();
    }

    public void startConsumer2() {
        System.out.println("start consumer 2");
        Consumer consumer = new Consumer("127.0.0.1");
        consumer.setConsumerListener(msg -> System.out.println("consumer 2"));
        consumer.setTopic("topic_1");
        consumer.setConsumerGroup("consumer_group_2");
        consumer.start();
    }

}