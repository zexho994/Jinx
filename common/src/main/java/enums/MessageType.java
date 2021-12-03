package enums;

import Message.Message;

import java.util.Objects;

/**
 * @author Zexho
 * @date 2021/11/18 5:24 下午
 */
public enum MessageType {

    /////////// Consumer ///////////////
    /**
     * consumer 执行 pullMessage 时发送的消息
     */
    Pull_Message("pullMessage"),

    /////////// Producer ///////////////
    Put_Message("message"),
    ;

    public final String type;

    MessageType(String type) {
        this.type = type;
    }

    public static MessageType get(String type) {
        for (MessageType t : MessageType.values()) {
            if (Objects.equals(t.type, type)) {
                return t;
            }
        }
        return null;
    }
}
