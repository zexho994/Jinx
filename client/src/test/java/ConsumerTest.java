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
        Consumer consumer = new Consumer();
        consumer.setConsumerListener(System.out::println);
        consumer.start();
    }
}