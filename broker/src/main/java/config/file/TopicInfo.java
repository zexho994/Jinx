package config.file;

import lombok.Data;
import lombok.ToString;

/**
 * @author Zexho
 * @date 2021/12/16 8:03 PM
 */
@Data
@ToString
public class TopicInfo {
    private String topic;
    private String queue;
}
