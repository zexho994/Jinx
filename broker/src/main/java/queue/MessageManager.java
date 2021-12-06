package queue;

import Message.Message;
import lombok.extern.log4j.Log4j2;
import store.DefaultMessageStore;
import store.FlushModel;
import store.MessageStore;

/**
 * @author Zexho
 * @date 2021/11/19 8:59 上午
 */
@Log4j2
public class MessageManager {

    private MessageManager() {
    }

    private static class Inner {
        private static final MessageManager INSTANCE = new MessageManager();
    }

    public static MessageManager getInstance() {
        return Inner.INSTANCE;
    }

    private final MessageStore messageStore = DefaultMessageStore.getInstance();

    /**
     * 投递消息
     */
    public void putMessage(Message message, FlushModel model) {
        // 消息交给存储模块进行存储
        messageStore.putMessage(message, model);
    }

}
