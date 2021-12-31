package enums;

import java.util.Objects;

/**
 * @author Zexho
 * @date 2021/12/10 8:00 下午
 */
public enum MessageResponseCode {

    /**
     * 成功
     */
    SUCCESS(1),
    /**
     * 失败
     */
    FAILURE(2);

    public final int code;

    MessageResponseCode(int code) {
        this.code = code;
    }

    public static MessageResponseCode get(int code) {
        for (MessageResponseCode t : MessageResponseCode.values()) {
            if (Objects.equals(t.code, code)) {
                return t;
            }
        }
        return null;
    }
}
