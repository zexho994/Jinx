package config.file;

import lombok.Data;
import lombok.ToString;

import java.util.List;

/**
 * @author Zexho
 * @date 2021/12/16 8:02 PM
 */
@Data
@ToString
public class BrokerInfo {
    private List<TopicInfo> topics;
}
