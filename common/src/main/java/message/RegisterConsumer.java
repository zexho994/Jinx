package message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author Zexho
 * @date 2021/12/28 4:50 PM
 */
@Data
@ToString
@AllArgsConstructor
public class RegisterConsumer implements Serializable {
    private static final long serialVersionUID = 5637298965306281876L;

    /**
     * 消费者id
     */
    private Integer cid;
    /**
     * 订阅的topic
     */
    private String topic;
    /**
     * 消费组
     */
    private String group;

}
