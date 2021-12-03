package topic;

import lombok.extern.log4j.Log4j2;
import queue.ConsumeQueue;

import java.util.HashMap;
import java.util.Map;

/**
 * 管理 topic
 *
 * @author Zexho
 * @date 2021/11/19 11:04 上午
 */
@Log4j2
public class TopicManager {

    private static final Map<String, ConsumeQueue> TOPIC_CONSUME_QUEUE_MAP = new HashMap<>();

    /**
     * 添加一个主题
     *
     * @param topic 要添加的主题名称
     */
    public static void addNewTopic(String topic) {
        if (TOPIC_CONSUME_QUEUE_MAP.containsKey(topic)) {
            return;
        }
        TOPIC_CONSUME_QUEUE_MAP.put(topic, new ConsumeQueue());
    }

    /**
     * 给topic添加一个订阅者消费组
     *
     * @param topic        topic 名称
     * @param consumeGroup 消费组名称
     */
    public static void addSubscriber(String topic, String consumeGroup) {
        ConsumeQueue consumeQueue = TOPIC_CONSUME_QUEUE_MAP.get(topic);
        if (consumeQueue == null) {
            consumeQueue = new ConsumeQueue();
            TOPIC_CONSUME_QUEUE_MAP.put(topic, consumeQueue);
            log.info("topic {} add subscriber {} success", topic, consumeGroup);
        } else {
            log.warn("consumeGroup {} is already exists on topic {}", consumeGroup, topic);
        }
    }

    /**
     * 获取 topic 的所有订阅者
     * 如果没有则创建一个
     *
     * @param topic 主题名称
     * @return 订阅了 {@param topic} 的所有消费组的队列
     */
    public static ConsumeQueue getConsumeQueue(String topic, String consumeGroup) {
        // 如果不存在改topic，则保存
        if (!TOPIC_CONSUME_QUEUE_MAP.containsKey(topic)) {
            addSubscriber(topic, consumeGroup);
        }
        return TOPIC_CONSUME_QUEUE_MAP.get(topic);
    }


}
