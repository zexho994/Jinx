
import Message.Message;
import Message.MessageStatusEnum;
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
        Message message = new Message(MessageStatusEnum.SUCCESS);
        while (true) {
            try {
                producer.sendMessageSync(message);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
