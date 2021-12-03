package queue;

import Message.Message;
import lombok.extern.log4j.Log4j2;
import store.DefaultMessageStore;
import store.FlushModel;
import store.MessageStore;
import topic.TopicManager;

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
     * Key : topic
     * Val : List<{@link ConsumeQueue}>
     */
    public Message pullMessage(String topic) {
        ConsumeQueue consumeQueues = TopicManager.getConsumeQueue(topic);
        return consumeQueues.pollMessage();
    }

    /**
     * 投递消息
     */
    public void putMessage(Message message, FlushModel model) {
        // 消息交给存储模块进行存储
        messageStore.putMessage(message, model);

        // 获取主题的所有消费组，所有消费组队列添加此消息
        String topic = message.getTopic();
        // 获取主题的消费队列
        ConsumeQueue consumeQueue = TopicManager.getConsumeQueue(topic);
        // 保存消息到消费队列
        consumeQueue.putMessage(message);
    }

}
