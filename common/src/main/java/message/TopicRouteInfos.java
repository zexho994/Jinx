package message;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author Zexho
 * @date 2021/12/27 4:42 PM
 */
@Data
public class TopicRouteInfos implements Serializable {

    private static final long serialVersionUID = -1504333813159921827L;

    private List<TopicRouteInfo> data;

    public TopicRouteInfos(List<TopicRouteInfo> topicRouteInfoList) {
        this.data = topicRouteInfoList;
    }
}
