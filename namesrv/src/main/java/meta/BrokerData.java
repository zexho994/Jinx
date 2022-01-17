package meta;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Zexho
 * @date 2021/12/22 7:47 PM
 */
@Data
@AllArgsConstructor
public class BrokerData {
    private String clusterName;
    private String brokerName;
    private String brokerHost;
    private Integer brokerPort;
}
