package message;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * @author Zexho
 * @date 2021/11/11 8:05 下午
 */
public class Message implements Serializable {
    private static final long serialVersionUID = 4450281597088189225L;

    /**
     * 每条消息唯一id
     */
    private String transactionId;

    /**
     * 消息所属topic
     */
    private String topic;

    private String consumerGroup;

    /**
     * 消息体
     */
    private Object body;

    public Message() {
        this(UUID.randomUUID().toString(), null, null);
    }

    public Message(String transactionId, String topic, byte[] body) {
        this.transactionId = transactionId;
        this.topic = topic;
        this.body = body;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
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

    @Override
    public String toString() {
        return "Message{" +
                "transactionId='" + transactionId + '\'' +
                ", topic='" + topic + '\'' +
                ", consumerGroup='" + consumerGroup + '\'' +
                ", body=" + body +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Message)) return false;
        Message message = (Message) o;
        return getTransactionId().equals(message.getTransactionId()) && getTopic().equals(message.getTopic()) && Objects.equals(getConsumerGroup(), message.getConsumerGroup()) && Objects.equals(getBody(), message.getBody());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTransactionId(), getTopic(), getConsumerGroup(), getBody());
    }
}
