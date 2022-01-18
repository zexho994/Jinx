package message;

/**
 * @author Zexho
 * @date 2021/11/18 5:20 下午
 */
public class PropertiesKeys {

    /**
     * 客户端类型
     * {@link enums.ClientType}
     */
    public static final String CLIENT_TYPE = "clientType";

    /**
     * 消息类型
     */
    public static final String MESSAGE_TYPE = "messageType";

    /**
     * broker服务的域名,在broker启动时设置
     */
    public static final String BROKER_HOST = "brokerHost";

    /**
     * broker名称，在broker启动时设置
     */
    public static final String BROKER_NAME = "brokerName";
    public static final String CLUSTER_NAME = "clusterName";
    public static final String BROKER_PORT = "brokerPort";
    public static final String BROKER_ID = "brokerId";

    /**
     * 事务标记
     */
    public static final String TRAN = "tran";
    public static final String RESP_DATA = "respData";
    public static final String TOPIC = "topic";
}
