package enums;

import java.util.Objects;

/**
 * @author Zexho
 * @date 2021/11/18 4:50 下午
 */
public enum ClientType {
    /**
     * 生产者身份
     */
    Producer("producer"),
    /**
     * 消费者身份
     */
    Consumer("consumer"),
    /**
     * broker身份
     */
    Broker("broker");

    public final String type;

    ClientType(String type) {
        this.type = type;
    }

    public static ClientType get(String type) {
        for (ClientType t : ClientType.values()) {
            if (Objects.equals(t.type, type)) {
                return t;
            }
        }
        return null;
    }
}
