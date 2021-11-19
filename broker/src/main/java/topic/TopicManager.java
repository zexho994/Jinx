package topic;

import lombok.extern.log4j.Log4j2;
import queue.MessageQueue;

import java.util.*;

/**
 * 管理 topic
 *
 * @author Zexho
 * @date 2021/11/19 11:04 上午
 */
@Log4j2
public class TopicManager {

    private static final Map<String, List<MessageQueue>> TOPIC_CACHE = new HashMap<>();

    public static void addNewTopic(String topic) {
        if (TOPIC_CACHE.containsKey(topic)) {
            return;
        }
        TOPIC_CACHE.put(topic, new LinkedList<>());
    }

    public static void addSubscriber(String topic, String consumeGroup) {
        List<MessageQueue> messageQueues = TOPIC_CACHE.get(topic);
        if (messageQueues == null) {
            List<MessageQueue> messageQueueList = new LinkedList<>();
            messageQueueList.add(new MessageQueue(consumeGroup));
            TOPIC_CACHE.put(topic, messageQueueList);
        } else if (messageQueues.stream().anyMatch(queue -> Objects.equals(queue.consumerGroup(), consumeGroup))) {
            log.info("consumeGroup already exists");
        } else {
            MessageQueue messageQueue = new MessageQueue(consumeGroup);
            messageQueues.add(messageQueue);
        }
    }

    public static List<MessageQueue> getTopicSubscriber(String topic) {
        if (!TOPIC_CACHE.containsKey(topic)) {
            return Collections.emptyList();
        }
        return TOPIC_CACHE.get(topic);
    }


}
