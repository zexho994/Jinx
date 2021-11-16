package consumer;

import netty.server.NettyRemotingServerImpl;
import netty.server.NettyServerConfig;

/**
 * @author Zexho
 * @date 2021/11/15 7:33 下午
 */
public class Consumer {

    private final NettyRemotingServerImpl nettyRemotingServer;
    private ConsumerListener consumerListener;

    public Consumer() {
        NettyServerConfig config = new NettyServerConfig();
        this.nettyRemotingServer = new NettyRemotingServerImpl(config);
    }

    public void start() {
        ConsumerHandler consumerHandler = new ConsumerHandler(this.consumerListener);
        this.nettyRemotingServer.setServerHandler(consumerHandler);
        this.nettyRemotingServer.start();
    }

    public void shutdown() {
        this.nettyRemotingServer.shutdown();
    }

    public void setConsumerListener(ConsumerListener consumerListener) {
        this.consumerListener = consumerListener;
    }

}
