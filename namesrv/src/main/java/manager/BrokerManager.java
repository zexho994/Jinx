package manager;

import model.BrokerData;
import utils.Broker;

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

    public final static Map<String/*cluster name*/, Map<String/*broker name*/, Map<String/*master or slave*/, BrokerData>>> CLUSTER_BROKER_ROLE_MAP = new ConcurrentHashMap<>();
    public final static Map<String/*broker name*/, BrokerData> BROKER_INFO = new ConcurrentHashMap<>();
    public static final String MASTER_FLAG = "MASTER";
    public static final String SLAVE_FLAG = "SLAVE";

    public void addBroker(String brokerName, String brokerHost, Integer brokerPort, Integer brokerId, String clusterName) {
        BrokerData brokerData = new BrokerData(clusterName, brokerName, brokerHost, brokerId, brokerPort);
        BROKER_INFO.put(brokerName, brokerData);

        if (CLUSTER_BROKER_ROLE_MAP.get(clusterName) == null)
            CLUSTER_BROKER_ROLE_MAP.put(clusterName, new ConcurrentHashMap<>());
        if (CLUSTER_BROKER_ROLE_MAP.get(clusterName).get(brokerName) == null)
            CLUSTER_BROKER_ROLE_MAP.get(clusterName).put(brokerName, new ConcurrentHashMap<>());
        if (Broker.isMaster(brokerId)) {
            CLUSTER_BROKER_ROLE_MAP.get(clusterName).get(brokerName).put(MASTER_FLAG, brokerData);
        } else {
            CLUSTER_BROKER_ROLE_MAP.get(clusterName).get(brokerName).put(SLAVE_FLAG, brokerData);
        }
    }

    public BrokerData getBrokerData(String brokerName) {
        return BROKER_INFO.get(brokerName);
    }

    public BrokerData getMasterData(String clusterName, String brokerName) {
        return CLUSTER_BROKER_ROLE_MAP.get(clusterName).get(brokerName).get(MASTER_FLAG);
    }

}
