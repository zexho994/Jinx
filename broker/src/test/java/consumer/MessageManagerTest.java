package consumer;

import Message.Message;
import org.junit.jupiter.api.Test;
import queue.MessageManager;
import store.FlushModel;

import java.util.UUID;

class MessageManagerTest {

    MessageManager messageManager = MessageManager.getInstance();

    @Test
    String putMessage(String topic, String group) {
        String msgId = UUID.randomUUID().toString();

        Message message = new Message();
        message.setTopic(topic);
        message.setConsumerGroup(group);
        message.setTransactionId(msgId);
        message.setBody(1);
        messageManager.putMessage(message, FlushModel.SYNC);

        return msgId;
    }
}