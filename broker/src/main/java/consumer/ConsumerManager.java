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

    /**
     * 消费消息
     * step1: 获取group在消费队列的消费序号
     * step2: 根据消费序号在消费队列中找到commitlog offset
     * step3: 根据commitlog offset在commitlog文件中找到对象消息
     *
     * @param topic        消息主题
     * @param consumeGroup 消费组
     * @return 未消费的消息
     */
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