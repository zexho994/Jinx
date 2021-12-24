package consumer;

import enums.ClientType;
import enums.MessageType;
import io.netty.channel.Channel;
import message.Message;
import message.PropertiesKeys;
import netty.client.NettyClientConfig;
import netty.client.NettyRemotingClientImpl;
import netty.protocal.RemotingCommand;
import remoting.RemotingService;
import utils.ByteUtil;

/**
 * @author Zexho
 * @date 2021/11/15 7:33 下午
 */
public class Consumer implements RemotingService {

    private final NettyRemotingClientImpl consumerClient;
    private ConsumerListener consumerListener;
    private PullConsumer pullConsumer;

    private String topic;
    private int queueId;
    private String consumerGroup;

    public Consumer(String host) {
        this.consumerClient = new NettyRemotingClientImpl(new NettyClientConfig(host));
    }

    @Override
    public void start() {
        ConsumerHandler consumerHandler = new ConsumerHandler(this.consumerListener);
        this.consumerClient.setClientHandler(consumerHandler);
        this.consumerClient.start();

        this.pullConsumer = new PullConsumer();
        pullConsumer.pullMessageTask(this);
    }

    @Override
    public void shutdown() {
        this.pullConsumer.shutdownTask();
        this.consumerClient.shutdown();
    }

    @Override
    public void sendMessage(Message message) {
        Channel channel = this.consumerClient.getChannel();

        RemotingCommand remotingCommand = new RemotingCommand();
        remotingCommand.setBody(ByteUtil.to(message));
        remotingCommand.addProperties(PropertiesKeys.CLIENT_TYPE, ClientType.Consumer.type);
        remotingCommand.addProperties(PropertiesKeys.MESSAGE_TYPE, MessageType.Pull_Message.type);

        channel.writeAndFlush(remotingCommand);
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerListener(ConsumerListener consumerListener) {
        this.consumerListener = consumerListener;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }
}
