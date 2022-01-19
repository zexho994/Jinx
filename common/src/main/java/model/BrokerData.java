package model;

import lombok.Data;

import java.io.Serializable;

/**
 * @author Zexho
 * @date 2021/12/22 7:47 PM
 */
@Data
public class BrokerData implements Serializable {
    private static final long serialVersionUID = 3702547387281811555L;

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
