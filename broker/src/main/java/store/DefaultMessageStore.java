package store;

import lombok.extern.log4j.Log4j2;
import message.Message;
import store.commitlog.Commitlog;
import store.constant.FlushModel;
import store.constant.PutMessageResult;
import store.consumequeue.ConsumeQueue;
import store.model.CommitPutMessageResult;

import static common.Transaction.TRANS_HALF_OP_TOPIC;
import static common.Transaction.TRANS_HALF_TOPIC;

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
     * 存储消息
     * step1: 检查存储状态
     * step2:
     *
     * @param message    消息对象
     * @param flushModel 刷盘模式
     * @return 存储执行结果
     */
    @Override
    public PutMessageResult putMessage(final Message message, final FlushModel flushModel) {
        log.trace("default message store message");
        if (!this.prepareCheck(message)) {
            return PutMessageResult.FAILURE;
        }
        return put(message, message.getTopic(), message.getQueueId(), flushModel);
    }

    /**
     * 存储
     *
     * @param message 消息对象
     * @param model   刷盘模式
     * @return 存储执行结果
     */
    @Override
    public PutMessageResult putHalfMessage(Message message, FlushModel model) {
        if (!this.prepareCheck(message)) {
            return PutMessageResult.FAILURE;
        }
        return put(message, TRANS_HALF_TOPIC, 1, model);
    }

    @Override
    public PutMessageResult putOpMessage(Message message, FlushModel model) {
        if (!this.prepareCheck(message)) {
            return PutMessageResult.FAILURE;
        }
        return put(message, TRANS_HALF_OP_TOPIC, 1, model);
    }

    /**
     * 执行存储
     * step1: 存储到commitlog中
     * step2: 存储到consumeQueue中
     *
     * @param message 消息对象
     * @param topic   要存储到的topic队列名称
     * @param queueId 目标队列id
     * @param model   刷盘模式
     * @return 执行结果
     */
    private PutMessageResult put(Message message, String topic, int queueId, FlushModel model) {
        // 交给commitlog进行存储
        CommitPutMessageResult commitlogPutResult = this.commitlog.putMessage(message, model);

        // 交给 consumeQueue 进行存储
        if (commitlogPutResult.getResult() == PutMessageResult.OK) {
            return this.consumeQueue.putMessage(topic, queueId, commitlogPutResult.getOffset());
        } else {
            log.error("Failed to put message to consumeQueue, topic = {}, queueId = {},commit offset = {}", topic, queueId, commitlogPutResult.getOffset());
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

    /**
     * 预检查
     *
     * @param message 消息对象
     * @return 检查结果
     */
    private boolean prepareCheck(Message message) {
        // 检查存储状态
        if (!this.checkStoreStatus()) {
            log.error("Store status is abnormality");
            return false;
        }
        // 检查消息格式
        if (!this.checkMessage(message)) {
            log.error("The Message format is invalid");
            return false;
        }
        return true;
    }

    /**
     * 检查当前的存储状态
     *
     * @return 检查结果
     */
    private boolean checkStoreStatus() {
        return true;
    }

    /**
     * 检查消息的合法性
     *
     * @param message 消息对象
     * @return 检查结果
     */
    private boolean checkMessage(Message message) {
        return message != null;
    }

}
