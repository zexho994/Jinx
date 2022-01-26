package message;

import lombok.Data;

import java.io.Serializable;
import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicUnit)) return false;
        TopicUnit topicUnit = (TopicUnit) o;
        return getTopic().equals(topicUnit.getTopic());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTopic());
    }
}
