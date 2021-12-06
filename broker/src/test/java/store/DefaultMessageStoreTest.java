package store;

import Message.Message;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

class DefaultMessageStoreTest {

    Commitlog commitlog = Commitlog.getInstance();
    DefaultMessageStore defaultMessageStore = DefaultMessageStore.getInstance();
    ConsumeQueue consumeQueue = ConsumeQueue.getInstance();

    {
        commitlog.init();
        consumeQueue.init();
    }

    @Test
    public void test() throws IOException {
        DefaultMessageStoreTest defaultMessageStoreTest = new DefaultMessageStoreTest();

        Message message1 = new Message();
        message1.setBody(1);
        message1.setTopic("message 1");
        message1.setConsumerGroup("message 1");
        defaultMessageStoreTest.putMessage(message1); // offset = 0,size = 316,commitoffset = 0

        Message message2 = new Message();
        message2.setBody("2");
        message2.setTopic("message 2");
        message2.setConsumerGroup("message 2");
        defaultMessageStoreTest.putMessage(message2); // offset =320,size = 316,commitlogoffset = 320


        Message message3 = new Message();
        message3.setBody(3L);
        message3.setTopic("message 3");
        message3.setConsumerGroup("message 3");
        defaultMessageStoreTest.putMessage(message3); // offset = 640,size = 316,commitlogoffset = 640

        Message message_1 = defaultMessageStoreTest.getMessage(0);// commitlogoffset = 0
        Message message_2 = defaultMessageStoreTest.getMessage(1); // commitlogoffset = 1
        Message message_3 = defaultMessageStoreTest.getMessage(2); // commitlogoffset = 2

        assert message1.equals(message_1);
        assert message2.equals(message_2);
        assert message3.equals(message_3);
    }


    void putMessage(Message message) {
        message.setTransactionId(UUID.randomUUID().toString());
        message.setConsumerGroup("consume group 1");
        message.setTopic("topic 1");
        message.setBody("{}");
        defaultMessageStore.putMessage(message);
    }

    Message getMessage(int seq) throws IOException {
        long commitlogOffset = consumeQueue.getCommitlogOffset("topic 1", seq);
        return commitlog.getMessage(commitlogOffset);
    }

}