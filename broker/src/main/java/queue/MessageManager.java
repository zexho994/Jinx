package queue;

import Message.Message;
import lombok.extern.log4j.Log4j2;
import store.Commitlog;
import topic.TopicManager;

import java.util.List;
import java.util.Optional;

/**
 * @author Zexho
 * @date 2021/11/19 8:59 上午
 */
@Log4j2
public enum MessageManager {


    /**
     * 实例对象
     */
    Instance;

    private Commitlog commitlog;

    /**
     * Key : topic
     * Val : List<{@link MessageQueue}>
     */
    public Message pullMessage(String topic, String consumerGroup) {
        List<MessageQueue> messageQueues = TopicManager.getTopicSubscriber(topic);
        Optional<MessageQueue> queue = messageQueues.stream().filter(cg -> cg.consumerGroup().equals(consumerGroup)).findFirst();
        if (queue.isPresent()) {
            return queue.get().poll();
        } else {
            TopicManager.addSubscriber(topic, consumerGroup);
            return null;
        }
    }

    /**
     * 投递消息
     */
    public void putMessage(Message message) {
        // 落盘
        boolean storeFlag = commitlog.storeMessage(message);
        if (!storeFlag) {
            log.warn("Failed to store message to file");
        }

        // 获取主题的所有消费组，所有消费组队列添加此消息
        String topic = message.getTopic();
        List<MessageQueue> messageQueues = TopicManager.getTopicSubscriber(topic);
        messageQueues.forEach(queue -> queue.put(message));
    }

}
