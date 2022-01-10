package message;

/**
 * @author Zexho
 * @date 2022/1/8 2:19 PM
 */
public enum TranType {
    /**
     * 事务的half消息
     */
    Half("1"),
    /**
     * 本地事务处理成功
     */
    Commit("2"),
    /**
     * 本地事务处理失败
     */
    Rollback("3");

    public final String type;

    TranType(String type) {
        this.type = type;
    }
}
