package config;

import lombok.Data;
import lombok.ToString;

import java.util.List;

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
     * topic配置
     */
    public List<TopicInfoUnit> topics;
}
