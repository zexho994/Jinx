package producer;

import lombok.extern.log4j.Log4j2;
import message.Message;
import message.PropertiesKeys;
import message.TranType;
import netty.protocal.RemotingCommand;
import store.DefaultMessageStore;
import store.MessageStore;
import store.constant.FlushModel;
import store.constant.PutMessageResult;

/**
 * @author Zexho
 * @date 2021/12/7 5:39 下午
 */
@Log4j2
public class ProducerManager {
    private ProducerManager() {
    }

    private static class Inner {
        private static final ProducerManager INSTANCE = new ProducerManager();
    }

    public static ProducerManager getInstance() {
        return ProducerManager.Inner.INSTANCE;
    }

    private final MessageStore messageStore = DefaultMessageStore.getInstance();

    /**
     * 投递普通消息
     * step1: 进行消息存储
     * step2: 获取topic的所有订阅consumer
     * step3: 执行推送到这些consumer
     * step4: 更新consumer的consume offset
     *
     * @param message 消息体
     * @param model   消息刷盘模式
     */
    public PutMessageResult messageProcessor(Message message, FlushModel model) {
        log.debug("put message : {}", message);
        // 消息交给存储模块进行存储
        return messageStore.putMessage(message, model);
    }

    public PutMessageResult transactionMessageProcessor(RemotingCommand command, FlushModel model) {
        TranType tranType = TranType.get(command.getProperty(PropertiesKeys.TRAN));
        Message message = command.getBody();

        // 根据事务消息的类型进行处理
        switch (tranType) {
            case Half:
                return this.halfMessageProcessor(message, model);
            case Commit:
                return this.commitProcessor(message, model);
            case Rollback:
                return this.rollbackProcessor(message, model);
            default:
                return PutMessageResult.FAILURE;
        }
    }

    /**
     * @param message 消息对象
     * @param model
     * @return 处理结果
     */
    private PutMessageResult rollbackProcessor(Message message, FlushModel model) {
        return this.messageStore.putOpMessage(message, model);
    }

    /**
     * 事务commit提交
     * step1: 消息保存一份到原本的队列中去
     * step2: 消息保存一份到opQueue中
     *
     * @param message 消息对象
     * @return 处理结果
     */
    private PutMessageResult commitProcessor(Message message, FlushModel model) {
        // 消息存储到正常的topic中
        PutMessageResult putResult = messageStore.putMessage(message, model);
        if (putResult != PutMessageResult.OK) {
            return putResult;
        }

        // 消息存储到opQueue中
        return messageStore.putOpMessage(message, model);
    }

    /**
     * 投递事务消息
     * step: 消息存储到TRANS_HALF_TOPIC队列中
     *
     * @param message half消息
     * @param model   消息刷盘模式
     */
    private PutMessageResult halfMessageProcessor(Message message, FlushModel model) {
        log.debug("put half message : {}", message);
        // 消息持久化操作
        return messageStore.putHalfMessage(message, model);
    }

}
