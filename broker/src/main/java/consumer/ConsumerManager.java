package consumer;

import message.Message;
import store.DefaultMessageStore;
import store.MessageStore;
import store.consumequeue.ConsumeQueue;

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

    private final MessageStore messageStore = DefaultMessageStore.getInstance();
    private final ConsumeQueue consumeQueue = ConsumeQueue.getInstance();


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
    public Message pullMessage(String topic, String consumeGroup) {
        Message message = messageStore.findMessage(topic, consumeGroup);
        if (message != null) {
            consumeQueue.incOffset(topic, consumeGroup);
        }
        return message;
    }
}
