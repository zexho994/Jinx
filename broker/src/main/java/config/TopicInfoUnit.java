package config;

import lombok.Data;

import java.io.Serializable;

/**
 * @author Zexho
 * @date 2021/12/22 7:23 PM
 */
@Data
public class TopicInfoUnit implements Serializable {
    private static final long serialVersionUID = 6708065748466902809L;

    /**
     * topic名称
     */
    private String topic;

    /**
     * 队列数量
     */
    private int queue;
}
