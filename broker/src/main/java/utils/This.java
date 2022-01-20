package utils;

import config.BrokerConfig;

/**
 * @author Zexho
 * @date 2022/1/20 8:49 AM
 */
public class This {

    public static boolean isMaster() {
        return Broker.isMaster(BrokerConfig.brokerId);
    }

}
