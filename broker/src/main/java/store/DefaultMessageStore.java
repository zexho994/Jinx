package store;

import Message.Message;
import lombok.extern.log4j.Log4j2;

/**
 * @author Zexho
 * @date 2021/12/2 9:26 上午
 */
@Log4j2
public enum DefaultMessageStore implements MessageStore {

    /**
     * 对象实例
     */
    INSTANCE;
    private final Commitlog commitlog = Commitlog.Instance;
    private final ConsumeQueue consumeQueue = ConsumeQueue.INSTANCE;

    /**
     * 默认采用同步的方式
     *
     * @param message 消息对象
     */
    @Override
    public void putMessage(Message message) {
        this.putMessage(message, FlushModel.SYNC);
    }

    /**
     * step1: 检查存储状态
     * step2:
     *
     * @param message    消息对象
     * @param flushModel 刷盘模式
     */
    @Override
    public void putMessage(final Message message, final FlushModel flushModel) {
        // 检查存储状态
        if (!this.checkStoreStatus()) {
            log.error("Store status is abnormality");
            return;
        }

        // 检查消息格式
        if (!this.checkMessage()) {
            log.error("The Message format is invalid");
            return;
        }

        // message -> inner message
        InnerMessage innerMessage = new InnerMessage(message);

        // 交给commitlog进行存储
        CommitPutMessageResult commitlogPutResult = this.commitlog.putMessage(innerMessage, flushModel);

        // 交给 consumeQueue 进行存储
        if (commitlogPutResult.getResult() == PutMessageResult.OK) {
            this.consumeQueue.putMessage(message, commitlogPutResult.getOffset(), commitlogPutResult.getMsgSize());
        } else if (commitlogPutResult.getResult() == PutMessageResult.FAILURE) {
            log.error("");
        }

    }

    public boolean checkStoreStatus() {
        return true;
    }

    public boolean checkMessage() {
        return true;
    }
}
