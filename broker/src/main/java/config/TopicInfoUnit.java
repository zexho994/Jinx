package config;

import lombok.Data;

/**
 * @author Zexho
 * @date 2021/12/22 7:23 PM
 */
@Data
public class TopicInfoUnit {
    /**
     * topic名称
     */
    private String topic;

    /**
     * 队列数量
     */
    private int queue;
}
