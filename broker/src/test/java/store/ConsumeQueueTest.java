package store;

import Message.Message;
import org.junit.jupiter.api.Test;
import store.consumequeue.ConsumeQueue;

import java.io.IOException;

class ConsumeQueueTest {

    ConsumeQueue consumeQueue = ConsumeQueue.getInstance();

    @Test
    void test() throws Exception {
        ConsumeQueueTest consumeQueueTest = new ConsumeQueueTest();
        consumeQueueTest.putMessage();
        consumeQueueTest.getMessage();
    }

    void putMessage() {
        consumeQueue.putMessage("test_a", 0);
        consumeQueue.putMessage("test_a", 100);
        consumeQueue.putMessage("test_a", 200);
        consumeQueue.putMessage("test_a", 300);
    }

    void getMessage() throws Exception {
        long l1 = consumeQueue.getMessageOffset("test_a", "");
        assert l1 == 0;
        long l2 = consumeQueue.getMessageOffset("test_a", "");
        assert l2 == 100;
        long l3 = consumeQueue.getMessageOffset("test_a", "");
        assert l3 == 200;
        long l4 = consumeQueue.getMessageOffset("test_a", "");
        assert l4 == 300;
    }

}