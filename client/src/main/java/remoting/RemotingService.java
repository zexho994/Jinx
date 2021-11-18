package remoting;

import Message.Message;

/**
 * @author Zexho
 * @date 2021/11/15 7:34 下午
 */
public interface RemotingService {

    void start();

    void shutdown();

    void sendMessage(Message message);
}
