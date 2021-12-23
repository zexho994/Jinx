import message.TopicUnit;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Zexho
 * @date 2021/12/22 7:51 PM
 */
public class TopicManager {
    private TopicManager() {
        brokerTopicsTable = new ConcurrentHashMap<>();
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

    public void addTopic(String brokerName, List<TopicUnit> topics) {
        brokerTopicsTable.put(brokerName, topics);
    }

    public List<TopicUnit> getTopicsByBrokerName(String brokerName) {
        return brokerTopicsTable.get(brokerName);
    }

}
