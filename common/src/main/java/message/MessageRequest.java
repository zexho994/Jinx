package message;

import lombok.Data;

/**
 * @author Zexho
 * @date 2021/12/10 7:56 下午
 */
@Data
public class MessageRequest {

    /**
     * 消息体
     */
    private final Message message;
    /**
     * 发送时间
     */
    private long sendDate;
    /**
     * 重试次数
     */
    private int retryCount;

    public MessageRequest(Message message) {
        this.message = message;
        this.sendDate = System.currentTimeMillis();
        this.retryCount = 0;
    }

    public void incRetryCount() {
        this.retryCount++;
    }

    public void setSendDate(long date) {
        this.sendDate = date;
    }

}
