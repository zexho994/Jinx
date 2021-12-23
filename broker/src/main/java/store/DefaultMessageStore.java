package store;

import lombok.extern.log4j.Log4j2;
import message.Message;
import store.commitlog.Commitlog;
import store.constant.FlushModel;
import store.constant.PutMessageResult;
import store.consumequeue.ConsumeQueue;
import store.model.CommitPutMessageResult;

/**
 * @author Zexho
 * @date 2021/12/2 9:26 上午
 */
@Log4j2
public class DefaultMessageStore implements MessageStore {

    private DefaultMessageStore() {
    }

    private static class Inner {
        private static final DefaultMessageStore INSTANCE = new DefaultMessageStore();
    }

    public static DefaultMessageStore getInstance() {
        return Inner.INSTANCE;
    }

    private final Commitlog commitlog = Commitlog.getInstance();
    private final ConsumeQueue consumeQueue = ConsumeQueue.getInstance();

    /**
     * 默认采用同步的方式
     *
     * @param message 消息对象
     * @return
     */
    @Override
    public PutMessageResult putMessage(Message message) {
        return this.putMessage(message, FlushModel.SYNC);
    }

    /**
     * step1: 检查存储状态
     * step2:
     *
     * @param message    消息对象
     * @param flushModel 刷盘模式
     * @return
     */
    @Override
    public PutMessageResult putMessage(final Message message, final FlushModel flushModel) {
        // 检查存储状态
        if (!this.checkStoreStatus()) {
            log.error("Store status is abnormality");
            return PutMessageResult.FAILURE;
        }

        // 检查消息格式
        if (!this.checkMessage()) {
            log.error("The Message format is invalid");
            return PutMessageResult.FAILURE;
        }

        // 交给commitlog进行存储
        CommitPutMessageResult commitlogPutResult = this.commitlog.putMessage(message, flushModel);

        // 交给 consumeQueue 进行存储
        if (commitlogPutResult.getResult() == PutMessageResult.OK) {
            return this.consumeQueue.putMessage(message.getTopic(), message.getQueueId(), commitlogPutResult.getOffset());
        } else {
            log.error("Failed to put message to consumeQueue, topic = {}, commit offset = {}", message.getTopic(), commitlogPutResult.getOffset());
            return PutMessageResult.FAILURE;
        }
    }

    @Override
    public Message findMessage(String topic, int queueId, String group) {
        try {
            // 获取commitlog文件偏移量
            Long commitlogOffset = consumeQueue.getCommitlogOffset(topic, queueId, group);
            if (commitlogOffset == null) {
                return null;
            }
            // 根据偏移量在commitlog文件中获取消息
            return commitlog.getMessage(commitlogOffset);
        } catch (Exception e) {
            log.error("Failed to find message, topic = {}, group = {}, e = {}", topic, group, e);
            return null;
        }
    }

    public boolean checkStoreStatus() {
        return true;
    }

    public boolean checkMessage() {
        return true;
    }

}
