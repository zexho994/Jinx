package store;

import message.Message;
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