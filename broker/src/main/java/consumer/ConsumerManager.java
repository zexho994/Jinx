package consumer;

import Message.Message;
import store.Commitlog;
import store.ConsumeOffset;
import store.ConsumeQueue;

import java.io.IOException;

/**
 * @author Zexho
 * @date 2021/12/6 10:47 上午
 */
public class ConsumerManager {

    private ConsumerManager() {
    }

    private static class Inner {
        private static final ConsumerManager INSTANCE = new ConsumerManager();
    }

    public static ConsumerManager getInstance() {
        return ConsumerManager.Inner.INSTANCE;
    }

    private final ConsumeOffset consumeOffset = ConsumeOffset.getInstance();
    private final ConsumeQueue consumeQueue = ConsumeQueue.getInstance();
    private final Commitlog commitlog = Commitlog.getInstance();

    public Message pullMessage(String topic, String consumeGroup) throws Exception {

        int consumeQueueSeq;
        try {
            consumeQueueSeq = consumeOffset.getOffset(topic, consumeGroup);
        } catch (IOException e) {
            throw new Exception("get consume offset error. topic = " + topic + ", consumeGroup = " + consumeGroup, e);
        }

        long commitlogOffset;
        try {
            commitlogOffset = consumeQueue.getCommitlogOffset(topic, consumeQueueSeq);
        } catch (IOException e) {
            throw new Exception("get commitlog offset error,topic = " + topic + ", consumeQueueSeq = " + consumeQueueSeq, e);
        }

        Message message;
        try {
            message = commitlog.getMessage(commitlogOffset);
        } catch (IOException e) {
            throw new Exception("get message error,commitlogOffset = " + commitlogOffset, e);
        }

        if (message != null) {
            consumeOffset.incOffset(topic, consumeGroup);
        }
        return message;
    }
}
