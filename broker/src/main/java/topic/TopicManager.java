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

    private static final Map<String, List<MessageQueue>> TOPIC_CONSUMER_GROUP_CACHE = new HashMap<>();

    /**
     * 添加一个主题
     *
     * @param topic 要添加的主题名称
     */
    public static void addNewTopic(String topic) {
        if (TOPIC_CONSUMER_GROUP_CACHE.containsKey(topic)) {
            return;
        }
        TOPIC_CONSUMER_GROUP_CACHE.put(topic, new LinkedList<>());
    }

    /**
     * 给topic添加一个订阅者消费组
     *
     * @param topic        topic 名称
     * @param consumeGroup 消费组名称
     */
    public static void addSubscriber(String topic, String consumeGroup) {
        List<MessageQueue> messageQueues = TOPIC_CONSUMER_GROUP_CACHE.get(topic);
        if (messageQueues == null) {
            List<MessageQueue> messageQueueList = new LinkedList<>();
            messageQueueList.add(new MessageQueue(topic, consumeGroup));
            TOPIC_CONSUMER_GROUP_CACHE.put(topic, messageQueueList);
        } else if (messageQueues.stream().anyMatch(queue -> Objects.equals(queue.consumerGroup(), consumeGroup))) {
            log.info("consumeGroup already exists");
        } else {
            MessageQueue messageQueue = new MessageQueue(topic, consumeGroup);
            messageQueues.add(messageQueue);
        }
    }

    /**
     * 获取 topic 的所有订阅者
     *
     * @param topic 主题名称
     * @return 订阅了 {@param topic} 的所有消费组的队列
     */
    public static List<MessageQueue> getTopicSubscriber(String topic) {
        if (!TOPIC_CONSUMER_GROUP_CACHE.containsKey(topic)) {
            return Collections.emptyList();
        }
        return TOPIC_CONSUMER_GROUP_CACHE.get(topic);
    }


}
