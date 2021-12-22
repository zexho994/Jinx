package config;

import lombok.Data;
import lombok.ToString;

import java.util.List;

/**
 * @author Zexho
 * @date 2021/12/16 8:02 PM
 */
@Data
@ToString
public class BrokerConfig {

    /**
     * topic配置
     */
    private List<TopicConfig> topics;

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

    /**
     * nameserver 服务的端口
     */
    public static final int NAMESRV_PORT = 9876;

}
