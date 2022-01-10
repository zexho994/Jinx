package message;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.UUID;

/**
 * @author Zexho
 * @date 2021/11/11 8:05 下午
 */
@ToString
@EqualsAndHashCode
public class Message implements Serializable {
    private static final long serialVersionUID = 4450281597088189225L;

    /**
     * 消息id，每条消息特有
     */
    private final String msgId;

    /**
     * 消息所属topic
     */
    private String topic;

    /**
     * 消费队列id
     */
    private Integer queueId;

    /**
     * 消费组
     */
    private String consumerGroup;

    /**
     * 消息体
     */
    private Object body;

    public Message() {
        this(null);
    }

    public Message(String msgId) {
        this(null, null, msgId);
    }

    public Message(String topic, byte[] body, String msgId) {
        this.topic = topic;
        this.body = body;
        this.msgId = msgId == null ? UUID.randomUUID().toString() : msgId;
    }

    public String getMsgId() {
        return msgId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public Object getBody() {
        return body;
    }

    public void setBody(Object data) {
        this.body = data;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }
}
