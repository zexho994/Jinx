package enums;

import java.util.Objects;

/**
 * @author Zexho
 * @date 2021/11/18 5:24 下午
 */
public enum MessageType {

    /////////// Common ///////////////
    /**
     * 获取topic的路由信息
     */
    Get_Topic_Route("getTopicRoute"),

    /////////// Consumer ///////////////

    /**
     * consumer 执行 pullMessage 时发送的消息
     */
    Pull_Message("pullMessage"),
    Register_Consumer("registerConsumer"),

    /////////// Producer ///////////////
    Put_Message("message"),

    /////////// Broker ///////////////
    Register_Broker("registerBroker"),

    Register_Broker_Resp("registerBrokerResp");

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
