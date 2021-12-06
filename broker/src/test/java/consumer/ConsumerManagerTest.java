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
        // 添加3条消息
        MessageManagerTest messageManagerTest = new MessageManagerTest();
        String msgId1 = messageManagerTest.putMessage(topic, group);
        String msgId2 = messageManagerTest.putMessage(topic, group);
        String msgId3 = messageManagerTest.putMessage(topic, group);

        // 消费3个消息
        Message message1 = consumerManager.pullMessage(topic, group);
        Message message2 = consumerManager.pullMessage(topic, group);
        Message message3 = consumerManager.pullMessage(topic, group);
        try {
            // 消费时候没有新消息
            Message message4 = consumerManager.pullMessage(topic, group);
        } catch (Exception ignored) {
        }

        String msgId4 = messageManagerTest.putMessage(topic, group);
        Message message4 = consumerManager.pullMessage(topic, group);

        // 比较消息
        Assertions.assertEquals(msgId1, message1.getTransactionId());
        Assertions.assertEquals(msgId2, message2.getTransactionId());
        Assertions.assertEquals(msgId3, message3.getTransactionId());
        Assertions.assertEquals(msgId4, message4.getTransactionId());
    }

}