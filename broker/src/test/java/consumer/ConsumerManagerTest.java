package consumer;

import Message.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import store.Commitlog;
import store.ConsumeOffset;
import store.ConsumeQueue;

class ConsumerManagerTest {

    Commitlog commitlog = Commitlog.getInstance();
    ConsumeQueue consumeQueue = ConsumeQueue.getInstance();
    ConsumeOffset consumeOffset = ConsumeOffset.getInstance();

    {
        commitlog.init();
        consumeQueue.init();
        consumeOffset.init();
    }

    ConsumerManager consumerManager = ConsumerManager.getInstance();


    String topic = "topic_1";
    String group = "group_1";

    @Test
    void pullMessage() throws Exception {
        // 添加一个消息
        MessageManagerTest messageManagerTest = new MessageManagerTest();
        String msgId = messageManagerTest.putMessage(topic, group);

        // 消费一个消息
        Message message = consumerManager.pullMessage(topic, group);
        System.out.println(message);

        // 比较消息
        Assertions.assertEquals(msgId, message.getTransactionId());
    }

}