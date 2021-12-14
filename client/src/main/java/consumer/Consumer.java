package consumer;

import message.Message;
import io.netty.channel.Channel;
import netty.client.NettyClientConfig;
import netty.client.NettyRemotingClientImpl;
import remoting.RemotingService;

/**
 * @author Zexho
 * @date 2021/11/15 7:33 下午
 */
public class Consumer implements RemotingService {

    private final NettyRemotingClientImpl consumerClient;
    private ConsumerListener consumerListener;
    private PullConsumer pullConsumer;

    private String topic;
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
        channel.writeAndFlush(message);
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
}
