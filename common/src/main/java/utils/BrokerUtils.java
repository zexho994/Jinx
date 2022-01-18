package utils;

/**
 * @author Zexho
 * @date 2022/1/18 3:25 PM
 */
public class BrokerUtils {

    public static boolean isMaster(int brokerId) {
        return brokerId == 0;
    }

}
