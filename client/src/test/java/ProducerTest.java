
import Message.Message;
import producer.Producer;

/**
 * @author Zexho
 * @date 2021/11/16 8:59 上午
 */
public class ProducerTest {

    public static void main(String[] args) {
        ProducerTest test = new ProducerTest();
        test.startProducer();
    }

    public void startProducer() {
        Producer producer = new Producer("127.0.0.1");
        producer.start();

        Message message = new Message();
        message.setTopic("topic_1");
        message.setConsumerGroup("consumer_group_1");
        while (true) {
            producer.sendMessage(message);
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
