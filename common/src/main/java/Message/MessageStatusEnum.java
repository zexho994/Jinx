package Message;

/**
 * @author Zexho
 * @date 2021/11/11 8:10 下午
 */
public enum MessageStatusEnum {
    /**
     * 成功
     */
    SUCCESS(200),

    /**
     * 失败
     */
    ERROR(500);

    final int code;

    MessageStatusEnum(int code) {
        this.code = code;
    }
}
