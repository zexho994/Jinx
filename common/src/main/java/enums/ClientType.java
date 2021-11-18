package enums;

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
    Consumer("consumer");

    public final String type;

    ClientType(String type) {
        this.type = type;
    }
}
