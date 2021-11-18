package consumer;

import Message.Message;

/**
 * @author Zexho
 * @date 2021/11/16 8:55 上午
 */
@FunctionalInterface
public interface ConsumerListener {

    /**
     * 消息消费处理
     *
     * @param message 要处理的消息对象
     */
    void consume(Message message);

}
