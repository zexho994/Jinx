package store;

import Message.Message;

/**
 * @author Zexho
 * @date 2021/12/2 9:25 上午
 */
public interface MessageStore {

    /**
     * 存储消息
     *
     * @param message 消息对象
     */
    void putMessage(Message message);


    /**
     * 存储消息
     *
     * @param message    消息对象
     * @param flushModel 刷盘模式
     */
    void putMessage(Message message, FlushModel flushModel);
}
