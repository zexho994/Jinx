package store.consumequeue;

/**
 * @author Zexho
 * @date 2021/12/8 9:55 上午
 */
public class GetCommitlogOffset {
    public final long offset;
    public final int size;

    public GetCommitlogOffset(long offset, int size) {
        this.offset = offset;
        this.size = size;
    }
}
