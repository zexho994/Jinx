package topic;

import config.BrokerConfig;
import message.TopicUnit;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Zexho
 * @date 2021/12/16 7:32 PM
 */
public class TopicManager {

    private TopicManager() {
    }

    private static class Inner {
        private static final TopicManager INSTANCE = new TopicManager();
    }

    public static TopicManager getInstance() {
        return TopicManager.Inner.INSTANCE;
    }

    private List<TopicUnit> topics;
    private final Map<String, TopicUnit> topicUnitMap = new ConcurrentHashMap<>();
    private boolean isInit = false;

    /**
     * 获取broker所有topic信息
     */
    public List<TopicUnit> getTopics() {
        this.checkInit();
        return this.topics;
    }

    public TopicUnit getTopic(String topic) {
        this.checkInit();
        return topicUnitMap.get(topic);
    }

    private void checkInit() {
        if (!isInit) {
            this.topics = BrokerConfig.configBody.getTopics();
            topics.forEach(unit -> this.topicUnitMap.put(unit.getTopic(), unit));
            isInit = true;
        }
    }

}
