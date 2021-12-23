package message;

import lombok.Data;

import java.io.Serializable;

/**
 * @author Zexho
 * @date 2021/12/23 9:56 AM
 */
@Data
public class TopicUnit implements Serializable {
    private static final long serialVersionUID = -4034766843271352723L;

    /**
     * topic名称
     */
    private String topic = "default_topic";

    /**
     * 队列数量
     */
    private int queue = 4;
}
