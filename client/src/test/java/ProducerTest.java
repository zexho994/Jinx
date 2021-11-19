
import Message.Message;
import producer.Producer;

/**
 * @author Zexho
 * @date 2021/11/16 8:59 上午
 */
public class ProducerTest {

    public static void main(String[] args) {
        ProducerTest test = new ProducerTest();
        test.startProducer1();
    }

    public void startProducer1() {
        Producer producer = new Producer("127.0.0.1");
        producer.start();
        producer.registeredTopic("topic_1");
        producer.registeredTopic("topic_2");

        Message message1 = new Message();
        message1.setTopic("topic_1");
        Message message2 = new Message();
        message2.setTopic("topic_2");
        while (true) {
            producer.sendMessage(message1);
            producer.sendMessage(message2);
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
