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
    public String clusterName;

    /**
     * broker名称
     */
    public String brokerName;

    /**
     * broker标识
     */
    public Integer brokerId;

    /**
     * topic配置
     */
    public ConfigBody body;
}
