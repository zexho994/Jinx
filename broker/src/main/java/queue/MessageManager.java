package queue;

import Message.Message;

import java.util.*;

/**
 * @author Zexho
 * @date 2021/11/19 8:59 上午
 */
public class MessageManager {

    /**
     * Key : topic
     * Val : List<{@link MessageQueue}>
     */
    private static final Map<String, List<MessageQueue>> TOPIC_CONSUMER_GROUP_CACHE = new HashMap<>();

    public static Message pullMessage(String topic, String consumerGroup) {
        List<MessageQueue> messageQueues = TOPIC_CONSUMER_GROUP_CACHE.get(topic);
        Optional<MessageQueue> queue = messageQueues.stream().filter(cg -> cg.consumerGroup().equals(consumerGroup)).findFirst();
        if (queue.isPresent()) {
            return queue.get().poll();
        } else {
            throw new RuntimeException("consumerGroup is not found!");
        }
    }

    /**
     * 投递消息
     */
    public static void putMessage(Message message) {
        String topic = message.getTopic();
        String consumerGroup = message.getConsumerGroup();

        List<MessageQueue> messageQueues = TOPIC_CONSUMER_GROUP_CACHE.get(topic);
        messageQueues.stream()
                .filter(queue -> Objects.equals(queue.consumerGroup(), consumerGroup))
                .forEach(queue -> queue.put(message));
    }

}
