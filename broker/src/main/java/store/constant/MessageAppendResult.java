package store.constant;

/**
 * @author Zexho
 * @date 2021/12/2 9:51 上午
 */
public enum MessageAppendResult {

    /**
     * 追加成功
     */
    OK,

    /**
     * 空间不够
     */
    INSUFFICIENT_SPACE,

    /**
     * io操作错误
     */
    IO_EXCEPTION,

}
