package producer;

/**
 * @author Zexho
 * @date 2022/1/4 5:58 PM
 */
public enum LocalTransactionState {
    /**
     * 提交状态，表示事务完成
     */
    COMMIT,
    /**
     * 回滚状态，事务执行失败
     */
    ROLLBACK,
    /**
     * 未知状态，表示事务未执行结束
     */
    UNKNOWN;
}
