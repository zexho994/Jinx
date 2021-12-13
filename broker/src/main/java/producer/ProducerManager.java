package producer;

import message.Message;
import store.DefaultMessageStore;
import store.MessageStore;
import store.constant.FlushModel;
import store.constant.PutMessageResult;

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

    /**
     * 投递消息
     */
    public PutMessageResult putMessage(Message message, FlushModel model) {
        // 消息交给存储模块进行存储
        return messageStore.putMessage(message, model);
    }
}
