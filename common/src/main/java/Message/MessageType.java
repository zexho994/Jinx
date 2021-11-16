package Message;

/**
 * @author Zexho
 * @date 2021/11/11 8:10 下午
 */
public enum MessageType {
    /**
     * 成功
     */
    SUCCESS(200),

    /**
     * 失败
     */
    ERROR(500);

    final int code;

    MessageType(int code) {
        this.code = code;
    }
}
