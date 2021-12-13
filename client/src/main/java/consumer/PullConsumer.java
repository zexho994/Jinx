package consumer;

import message.Message;
import message.PropertiesKeys;
import enums.ClientType;
import enums.MessageType;
import lombok.extern.log4j.Log4j2;

/**
 * @author Zexho
 * @date 2021/11/18 4:55 下午
 */
@Log4j2
public class PullConsumer {

    private Consumer consumer;

    /**
     * 从 broker 拉取消息
     *
     * @param consumer
     */
    public void pullMessageTask(Consumer consumer) {
        Message pullRequest = new Message();
        pullRequest.addProperties(PropertiesKeys.MESSAGE_TYPE, MessageType.Pull_Message.type);
        pullRequest.setTopic(consumer.getTopic());
        pullRequest.setConsumerGroup(consumer.getConsumerGroup());
        pullRequest.addProperties(PropertiesKeys.CLIENT_TYPE, ClientType.Consumer.type);

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
