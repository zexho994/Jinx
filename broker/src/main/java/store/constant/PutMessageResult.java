package store.constant;

/**
 * @author Zexho
 * @date 2021/12/2 10:45 上午
 */
public enum PutMessageResult {
    /**
     * 成功
     */
    OK(1),
    /**
     * 失败
     */
    FAILURE(2),
    ;

    public final int code;

    PutMessageResult(int code) {
        this.code = code;
    }
}
