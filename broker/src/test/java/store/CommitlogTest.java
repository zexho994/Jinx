package store;

import Message.Message;
import org.junit.jupiter.api.Test;
import store.commitlog.Commitlog;
import store.constant.FlushModel;

class CommitlogTest {

    Commitlog commitlog = Commitlog.getInstance();


    @Test
    void putMessage() {
        Message message = new Message();
        commitlog.putMessage(message, FlushModel.SYNC);
    }

}