import consumer.Consumer;
import message.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import producer.LocalTransactionState;
import producer.TransactionListener;
import producer.TransactionMQProducer;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author Zexho
 * @date 2022/1/11 7:16 PM
 */
public class TransactionTest {

    static Set<Message> set = new CopyOnWriteArraySet<>();

    TransactionMQProducer startTransactionMQProducer() {
        TransactionMQProducer producer = new TransactionMQProducer("127.0.0.1");
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg) {
                System.out.println("start execute LocalTransaction success, message => " + msg);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("[Test] local Transaction success");
                return LocalTransactionState.COMMIT;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(Message msg) {
                System.out.println("[Test] checkLocalTransaction, message => " + msg);
                return LocalTransactionState.COMMIT;
            }
        });
        producer.start();
        return producer;
    }

    void startConsumer(String topic, String group, int cid) {
        Consumer consumer = new Consumer("127.0.0.1", cid);
        consumer.setTopic(topic);
        consumer.setConsumerGroup(group);
        consumer.setConsumerListener(msg -> {
            set.remove(msg);
            System.out.println("[Consumer] message => " + msg);
        });
        consumer.start();

    }

    /**
     * 用例名称:commit 测试
     * 前置:一个生产者，一个消费者
     * 步骤:
     * step1 : producer send message to broker
     * 测试点: 消费者此时收不到消息
     * step2 : 生产者执行本地事务成功，发送commit end消息
     * 测试点: 消费者收到消息
     */
    @Test
    public void transactionTest1() throws InterruptedException {
        // 启动消费者和生产者
        this.startConsumer("topic_1", "group_1", 1);
        TransactionMQProducer producer = this.startTransactionMQProducer();
        // 生产者发送消息
        Message message = new Message();
        message.setTopic("topic_1");
        set.add(message);
        new Thread(() -> {
            try {
                producer.sendMessage(message);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        Thread.sleep(1000);
        Assertions.assertTrue(set.contains(message), "事务消息被提前消费");
        Thread.sleep(1500);
        Assertions.assertFalse(set.contains(message), "事务消息未被正确消费掉");
    }

}
