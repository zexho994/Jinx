package store;

import Message.Message;
import org.junit.jupiter.api.Test;

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
        long l1 = consumeQueue.getCommitlogOffset("test_a", 0);
        assert l1 == 0;
        long l2 = consumeQueue.getCommitlogOffset("test_a", 1);
        assert l2 == 100;
        long l3 = consumeQueue.getCommitlogOffset("test_a", 2);
        assert l3 == 200;
        long l4 = consumeQueue.getCommitlogOffset("test_a", 3);
        assert l4 == 300;
    }

    @Test
    void testGetMessage() throws IOException {
        Message test_a = consumeQueue.getMessage("test_a", 0);
    }
}