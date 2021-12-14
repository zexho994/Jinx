package producer;

import message.Message;

/**
 * @author Zexho
 * @date 2021/12/13 2:20 下午
 */
@FunctionalInterface
public interface AfterRetryProcess {

    /**
     * 当消息发送失败，并重试最大次数后，执行该方法。
     * 该方法需要由客户端自己定义
     */
    void process(Message message);

}
