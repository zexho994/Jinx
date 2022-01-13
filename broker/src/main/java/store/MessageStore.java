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
     * @param message    消息对象
     * @param flushModel 刷盘模式
     * @return 存储执行结果
     */
    PutMessageResult putMessage(Message message, FlushModel flushModel);

    /**
     * 获取消息
     *
     * @param topic   消息主题
     * @param queueId 队列序号
     * @param group   消费组
     * @return 消息对象
     */
    Message findMessage(String topic, int queueId, String group);

    /**
     * 存储half消息
     *
     * @param message 消息对象
     * @return 存储执行结果
     */
    PutMessageResult putHalfMessage(Message message, FlushModel model);

    /**
     * 存储op消息中
     *
     * @param message 消息对象
     * @param model   刷盘模式
     * @return 存储结果
     */
    PutMessageResult putOpMessage(Message message, FlushModel model);
}
