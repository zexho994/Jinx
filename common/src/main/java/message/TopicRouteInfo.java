package message;

import lombok.Data;

import java.io.Serializable;

/**
 * @author Zexho
 * @date 2021/12/27 4:59 PM
 */
@Data
public class TopicRouteInfo implements Serializable {

    private static final long serialVersionUID = -8934593642862193248L;

    private String clusterName;
    private String brokerName;
    private String brokerHost;
    private Integer brokerPort;
    private String topic;
    private Integer queueNum;
}
