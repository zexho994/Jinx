package manager;

import meta.BrokerData;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Zexho
 * @date 2021/12/16 2:28 PM
 */
public class BrokerManager {

    private BrokerManager() {
    }

    private static class Inner {
        private static final BrokerManager INSTANCE = new BrokerManager();
    }

    public static BrokerManager getInstance() {
        return BrokerManager.Inner.INSTANCE;
    }

    /**
     * key = broker name, val = broker data obj
     */
    public final static Map<String, BrokerData> BROKER_INFO = new ConcurrentHashMap<>();


    public void addBroker(String brokerName, String brokerHost, String clusterName) {
        if (BROKER_INFO.containsKey(brokerName)) {
            return;
        }
        BROKER_INFO.put(brokerName, new BrokerData(brokerName, brokerHost, clusterName));
    }

    public BrokerData getBrokerData(String brokerName) {
        return BROKER_INFO.get(brokerName);
    }

}
