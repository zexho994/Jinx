
import message.Message;
import producer.Producer;

import java.util.UUID;

/**
 * @author Zexho
 * @date 2021/11/16 8:59 上午
 */
public class ProducerTest {

    public static void main(String[] args) {
        new Thread(() -> ProducerTest.startProducer1(30000)).start();
//        new Thread(() -> ProducerTest.startProducer2(1000)).start();
    }

    public static void startProducer1(int sleep) {
        Producer producer = new Producer("group_1", "127.0.0.1");
        producer.start();

        int n = 0;
        while (true) {
            Message message = new Message();
            // 自定义失败处理方法
            producer.setAfterRetryProcess(msg -> System.out.println("my after retry process method"));
            message.setTopic("topic_1");
            message.setTransactionId(UUID.randomUUID().toString());
            message.setBody(++n);
            producer.sendMessage(message);
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void startProducer2(int sleep) {
        Producer producer = new Producer("group_2", "127.0.0.1");
        producer.start();

        Message message2 = new Message();
        message2.setTopic("topic_2");
        int n = 0;
        while (true) {
            message2.setTransactionId(UUID.randomUUID().toString());
            message2.setBody(++n);
            producer.sendMessage(message2);
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
