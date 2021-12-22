package config;

import java.util.List;

/**
 * @author Zexho
 * @date 2021/12/16 8:02 PM
 */
public class BrokerConfig {

    /**
     * nameserver 服务的域名
     */
    public static String nameSrvHost;

    /**
     * 集群名称
     */
    public static String clusterName;

    /**
     * broker 服务的域名
     */
    public static String brokerHost;

    /**
     * broker 实例名称
     */
    public static String brokerName = "default_broker";

    public static List<TopicInfoUnit> topics;

    /**
     * nameserver 服务的端口
     */
    public static final int NAMESRV_PORT = 9876;

}
