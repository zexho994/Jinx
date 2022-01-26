package config;

import lombok.Data;
import lombok.ToString;
import message.ConfigBody;

/**
 * @author Zexho
 * @date 2021/12/22 7:17 PM
 */
@Data
@ToString
public class BrokerConfigFile {

    /**
     * 所属集群名称
     */
    private String clusterName;

    /**
     * broker名称
     */
    private String brokerName;

    /**
     * broker标识
     */
    private Integer brokerId;

    /**
     * topic配置
     */
    private ConfigBody body;
}
