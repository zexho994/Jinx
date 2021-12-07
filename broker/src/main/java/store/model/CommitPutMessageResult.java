package store.model;

import lombok.Data;
import store.constant.PutMessageResult;

/**
 * @author Zexho
 * @date 2021/12/2 10:35 上午
 */
@Data
public class CommitPutMessageResult {

    /**
     * 执行结果
     */
    private PutMessageResult result;
    /**
     * 消息所在文件中的偏移量
     */
    private int offset;
    /**
     * 消息的大小
     */
    private int msgSize;

    public CommitPutMessageResult(PutMessageResult result, int offset, int msgSize) {
        this.result = result;
        this.offset = offset;
        this.msgSize = msgSize;
    }

    public static CommitPutMessageResult ok(final int offset, final int msgSize) {
        return new CommitPutMessageResult(PutMessageResult.OK, offset, msgSize);
    }

    public static CommitPutMessageResult error() {
        return new CommitPutMessageResult(PutMessageResult.FAILURE, -1, -1);
    }
}
