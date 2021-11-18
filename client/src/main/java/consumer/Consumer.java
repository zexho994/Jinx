package consumer;

import netty.client.NettyClientConfig;
import netty.client.NettyRemotingClientImpl;

/**
 * @author Zexho
 * @date 2021/11/15 7:33 下午
 */
public class Consumer {

    private final NettyRemotingClientImpl consumerClient;
    private ConsumerListener consumerListener;

    public Consumer(String host) {
        this.consumerClient = new NettyRemotingClientImpl(new NettyClientConfig(host));
    }

    public void start() {
        ConsumerHandler consumerHandler = new ConsumerHandler(this.consumerListener);
        this.consumerClient.setClientHandler(consumerHandler);
        this.consumerClient.start();
    }

    public void shutdown() {
        this.consumerClient.shutdown();
    }

    public void setConsumerListener(ConsumerListener consumerListener) {
        this.consumerListener = consumerListener;
    }

}
