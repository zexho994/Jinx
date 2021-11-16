package consumer;

import Message.Message;

/**
 * @author Zexho
 * @date 2021/11/16 8:55 上午
 */
@FunctionalInterface
public interface ConsumerListener {

    /**
     * 消费
     */
    void consume(Message message);

}
