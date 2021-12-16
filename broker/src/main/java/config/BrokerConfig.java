package config;

/**
 * @author Zexho
 * @date 2021/12/16 9:50 AM
 */
public class BrokerConfig {
    /**
     * nameserver 服务的域名
     */
    public static String nameSrvHost;

    /**
     * broker 服务的域名
     */
    public static String brokerHost;

    /**
     * broker 实例名称
     */
    public static String brokerName = "default_broker";

    public static final int NAMESRV_PORT = 9876;
}
