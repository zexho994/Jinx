package meta;

import lombok.Data;

/**
 * @author Zexho
 * @date 2021/12/22 7:47 PM
 */
@Data
public class BrokerData {
    private String clusterName;
    private String brokerName;
    private String brokerHost;
    private Integer brokerPort;
    private Integer brokerId;

    public BrokerData(String clusterName, String brokerName, String brokerHost, Integer brokerId, Integer brokerPort) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.brokerHost = brokerHost;
        this.brokerId = brokerId;
        this.brokerPort = brokerPort;
    }
}
