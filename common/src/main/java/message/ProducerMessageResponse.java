package message;

import lombok.Data;

import java.io.Serializable;

/**
 * @author Zexho
 * @date 2021/12/10 7:58 下午
 */
@Data
public class ProducerMessageResponse implements Serializable {
    private static final long serialVersionUID = -4173281369560149136L;

    /**
     * 请求响应
     */
    private int code;

    /**
     * 每条消息唯一id
     */
    private String transactionId;

}
