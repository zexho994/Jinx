package enums;

/**
 * @author Zexho
 * @date 2021/11/18 5:24 下午
 */
public enum MessageType {

    /**
     * consumer 执行 pullMessage 时发送的消息
     */
    Pull_Message("pullMessage"),

    Message("message");

    public final String type;

    MessageType(String type) {
        this.type = type;
    }
}
