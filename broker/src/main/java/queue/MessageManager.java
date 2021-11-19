package queue;

import Message.Message;
import topic.TopicManager;

import java.util.List;
import java.util.Optional;

/**
 * @author Zexho
 * @date 2021/11/19 8:59 上午
 */
public class MessageManager {

    /**
     * Key : topic
     * Val : List<{@link MessageQueue}>
     */
    public static Message pullMessage(String topic, String consumerGroup) {
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
    public static void putMessage(Message message) {
        String topic = message.getTopic();

        // 获取主题的所有消费组，所有消费组队列添加此消息
        List<MessageQueue> messageQueues = TopicManager.getTopicSubscriber(topic);
        messageQueues.forEach(queue -> queue.put(message));
    }

}
