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

    private final Map<String/*cluster*/, Map<String/*broker name*/, Set<TopicUnit>>> brokerTopicsTable;
    private final Map<String/*topic*/, Map<String/*cluster name*/, Set<String>/*broker name(master)*/>> topicBrokerTable;

    public void addTopic(String clusterName, String brokerName, List<TopicUnit> topics) {
        if (!brokerTopicsTable.containsKey(clusterName)) {
            brokerTopicsTable.put(clusterName, new ConcurrentHashMap<>(8));
        }
        if (!brokerTopicsTable.get(clusterName).containsKey(brokerName)) {
            brokerTopicsTable.get(clusterName).put(brokerName, new CopyOnWriteArraySet<>());
        }
        Set<TopicUnit> existingTopics = brokerTopicsTable.get(clusterName).get(brokerName);
        existingTopics.addAll(topics);

        for (TopicUnit tu : topics) {
            if (!topicBrokerTable.containsKey(tu.getTopic())) {
                topicBrokerTable.put(tu.getTopic(), new ConcurrentHashMap<>());
            }
            if (!topicBrokerTable.get(tu.getTopic()).containsKey(clusterName)) {
                topicBrokerTable.get(tu.getTopic()).put(clusterName, new CopyOnWriteArraySet<>());
            }
            topicBrokerTable.get(tu.getTopic()).get(clusterName).add(brokerName);
        }

    }

    public Set<TopicUnit> getTopicsByBrokerName(String clusterName, String brokerName) {
        return brokerTopicsTable.get(clusterName).get(brokerName);
    }

    public Map<String, Set<String>> getBrokerNameByTopic(String topic) {
        return topicBrokerTable.get(topic);
    }

}
