import consumer.Consumer;

/**
 * @author Zexho
 * @date 2021/11/16 8:56 上午
 */
public class ConsumerTest {
    public static void main(String[] args) {
        ConsumerTest consumerTest = new ConsumerTest();
        consumerTest.startConsumer();
    }

    public void startConsumer() {
        Consumer consumer = new Consumer("127.0.0.1");
        consumer.setConsumerListener(System.out::println);
        consumer.setTopic("topic_1");
        consumer.setConsumerGroup("consumer_group_1");
        consumer.start();
    }
}