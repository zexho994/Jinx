package producer;

import consumer.ConsumerManager;
import message.Message;
import store.DefaultMessageStore;
import store.MessageStore;
import store.constant.FlushModel;
import store.constant.PutMessageResult;
import store.consumequeue.ConsumeQueue;

/**
 * @author Zexho
 * @date 2021/12/7 5:39 下午
 */
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
    private final ConsumeQueue consumeQueue = ConsumeQueue.getInstance();
    private final ConsumerManager consumerManager = ConsumerManager.getInstance();

    /**
     * 投递消息
     * step1: 进行消息存储
     * step2: 获取topic的所有订阅consumer
     * step3: 执行推送到这些consumer
     * step4: 更新consumer的consume offset
     *
     * @param message 消息体
     * @param model   消息刷盘模式
     */
    public PutMessageResult putMessage(Message message, FlushModel model) {
        // 消息交给存储模块进行存储
        PutMessageResult storeResult = messageStore.putMessage(message, model);

        // todo 判断客户端消费模式,pull or push
        // 消息执行推送
        consumerManager.doMessagePush(message.getTopic(), message.getQueueId(), message);

        return storeResult;
    }
}
