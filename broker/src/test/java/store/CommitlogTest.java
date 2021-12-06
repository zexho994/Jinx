package store;

import Message.Message;
import org.junit.jupiter.api.Test;

class CommitlogTest {

    Commitlog commitlog = Commitlog.getInstance();


    @Test
    void putMessage() {
        Message message = new Message();
        commitlog.putMessage(message, FlushModel.SYNC);
    }

    @Test
    void getMessage() {
    }
}