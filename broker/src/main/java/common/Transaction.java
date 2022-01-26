package common;

/**
 * @author Zexho
 * @date 2022/1/7 4:06 PM
 */
public class Transaction {
    /**
     * 事务消费队列名，存储half消息
     */
    public static final String TRANS_HALF_TOPIC = "TRANS_HALF_TOPIC";

    /**
     * 事务消费队列名，存储op消息
     */
    public static final String TRANS_HALF_OP_TOPIC = "TRANS_HALF_OP_TOPIC";

}
