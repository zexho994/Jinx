package config;

import message.ConfigBody;

import java.io.File;

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
     * brokerId,0表示master,大于0表示slave
     */
    public static Integer brokerId = 0;

    /**
     * broker 服务的监听端口
     */
    public static Integer brokerPort = 9944;

    /**
     * broker 实例名称
     */
    public static String brokerName = "default_broker";

    public static ConfigBody configBody;
    /**
     * broker配置文件磁盘路径
     */
    public static String brokerConfigPath = System.getProperty("user.home") + File.separator + "jinx" + File.separator + "config" + File.separator + "broker";
    /**
     * store模块配置文件磁盘路径
     */
    public static String storeConfigPath = System.getProperty("user.home") + File.separator + "jinx" + File.separator + "config" + File.separator + "store";
    /**
     * nameserver 服务的端口
     */
    public static final int NAMESRV_PORT = 9876;

}
