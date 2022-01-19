package manager;

import message.TopicUnit;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author Zexho
 * @date 2021/12/22 7:51 PM
 */
public class TopicManager {

    private TopicManager() {
        brokerTopicsTable = new ConcurrentHashMap<>();
        topicBrokerTable = new ConcurrentHashMap<>();
    }

    private static class Inner {
        private static final TopicManager INSTANCE = new TopicManager();
    }

    public static TopicManager getInstance() {
        return TopicManager.Inner.INSTANCE;
    }

    /**
     * key = broker name , val = topics
     */
    private final Map<String, List<TopicUnit>> brokerTopicsTable;

    private final Map<String, Set<String>> topicBrokerTable;

    public void addTopic(String brokerName, List<TopicUnit> topics) {
        brokerTopicsTable.put(brokerName, topics);
        for (TopicUnit tu : topics) {
            if (!topicBrokerTable.containsKey(tu.getTopic())) {
                topicBrokerTable.put(tu.getTopic(), new CopyOnWriteArraySet<>());
            }
            topicBrokerTable.get(tu.getTopic()).add(brokerName);
        }
    }

    public List<TopicUnit> getTopicsByBrokerName(String brokerName) {
        return brokerTopicsTable.get(brokerName);
    }

    public Set<String> getBrokerNameByTopic(String topic) {
        return topicBrokerTable.get(topic);
    }

}
