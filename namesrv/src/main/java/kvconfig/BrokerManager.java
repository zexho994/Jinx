package kvconfig;

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

    static class BrokerData {
        private final String name;
        private final String host;

        public BrokerData(String name, String host) {
            this.name = name;
            this.host = host;
        }
    }

    public void addBroker(String name, String host) {
        if (BROKER_INFO.containsKey(name)) {
            return;
        }
        BROKER_INFO.put(name, new BrokerData(name, host));
    }

    public BrokerData getBrokerData(String name) {
        return BROKER_INFO.get(name);
    }

}
