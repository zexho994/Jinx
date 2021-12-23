package store;

import message.Message;
import store.constant.FlushModel;
import store.constant.PutMessageResult;

/**
 * @author Zexho
 * @date 2021/12/2 9:25 上午
 */
public interface MessageStore {

    /**
     * 存储消息
     *
     * @param message 消息对象
     * @return
     */
    PutMessageResult putMessage(Message message);


    /**
     * 存储消息
     *
     * @param message    消息对象
     * @param flushModel 刷盘模式
     * @return
     */
    PutMessageResult putMessage(Message message, FlushModel flushModel);

    /**
     * 获取消息
     *
     * @param topic 消息主题
     * @param group 消费组
     * @return 消息对象
     */
    Message findMessage(String topic, int queueId, String group);

}
