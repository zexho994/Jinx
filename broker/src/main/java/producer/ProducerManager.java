package producer;

import common.Transaction;
import lombok.extern.log4j.Log4j2;
import message.Message;
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
    public PutMessageResult putMessage(Message message, FlushModel model) {
        log.debug("put message : {}", message);
        // 消息交给存储模块进行存储
        return messageStore.putMessage(message, model);
    }

    /**
     * 投递事务消息
     *
     * @param message half消息
     * @param model   消息刷盘模式
     */
    public PutMessageResult putHalfMessage(Message message, FlushModel model) {
        log.debug("put half message : {}", message);
        // 消息持久化操作
        messageStore.putHalfMessage(message, model);
        Message tt = messageStore.findMessage(Transaction.TRANS_HALF_TOPIC, 1, "tt");

        return PutMessageResult.OK;
    }

}
