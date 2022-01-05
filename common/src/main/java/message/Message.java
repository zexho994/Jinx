package message;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author Zexho
 * @date 2021/11/11 8:05 下午
 */
public class Message implements Serializable {
    private static final long serialVersionUID = 4450281597088189225L;

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
        this(null, null);
    }

    public Message(String topic, byte[] body) {
        this.topic = topic;
        this.body = body;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getConsumerGroup() {
        return consumerGroup;
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

    @Override
    public String toString() {
        return "Message{" +
                ", topic='" + topic + '\'' +
                ", queueId=" + queueId +
                ", consumerGroup='" + consumerGroup + '\'' +
                ", body=" + body +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Message)) return false;
        Message message = (Message) o;
        return Objects.equals(getTopic(), message.getTopic()) && Objects.equals(getQueueId(), message.getQueueId()) && Objects.equals(getConsumerGroup(), message.getConsumerGroup()) && Objects.equals(getBody(), message.getBody());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTopic(), getQueueId(), getConsumerGroup(), getBody());
    }
}
