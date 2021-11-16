package Message;

import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author Zexho
 * @date 2021/11/11 8:05 下午
 */
@ToString
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

    /**
     * 消息属性列表
     */
    private final Map<String, String> properties;

    /**
     * 消息体
     */
    private byte[] body;

    public Message(MessageStatusEnum messageStatusEnum) {
        this.transactionId = UUID.randomUUID().toString();
        this.properties = new HashMap<>();
        this.properties.put("status", String.valueOf(messageStatusEnum.code));
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

    public String getProperty(String key) {
        return properties.get(key);
    }

    public void addProperties(String key, String val) {
        this.properties.put(key, val);
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}
