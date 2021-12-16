package consumer;

import enums.ClientType;
import enums.MessageType;
import lombok.extern.log4j.Log4j2;
import message.Message;
import message.PropertiesKeys;
import netty.protocal.RemotingCommand;

/**
 * @author Zexho
 * @date 2021/11/18 4:55 下午
 */
@Log4j2
public class PullConsumer {

    /**
     * 从 broker 拉取消息
     */
    public void pullMessageTask(Consumer consumer) {
        Message pullRequest = new Message();
        pullRequest.setTopic(consumer.getTopic());
        pullRequest.setConsumerGroup(consumer.getConsumerGroup());


        while (true) {
            try {
                consumer.sendMessage(pullRequest);
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void shutdownTask() {
    }

}
