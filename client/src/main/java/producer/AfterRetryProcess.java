package producer;

import message.Message;

/**
 * @author Zexho
 * @date 2021/12/13 2:20 下午
 */
@FunctionalInterface
public interface AfterRetryProcess {
    void process(Message message);
}
