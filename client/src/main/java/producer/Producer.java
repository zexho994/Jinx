package producer;

import Message.Message;
import io.netty.channel.Channel;
import lombok.extern.log4j.Log4j2;
import netty.client.NettyClientConfig;
import netty.client.NettyRemotingClientImpl;
import remoting.RemotingService;

/**
 * @author Zexho
 * @date 2021/11/15 7:35 下午
 */
@Log4j2
public class Producer implements RemotingService {

    private final NettyRemotingClientImpl nettyRemotingClient;

    public Producer(String host) {
        NettyClientConfig config = new NettyClientConfig(host);
        this.nettyRemotingClient = new NettyRemotingClientImpl(config);
    }

    @Override
    public void start() {
        this.nettyRemotingClient.start();
    }

    @Override
    public void shutdown() {
        this.nettyRemotingClient.shutdown();
    }

    public void sendMessageSync(Message message) throws InterruptedException {
        log.info("SEND MESSAGE => {}", message);
        Channel channel = this.nettyRemotingClient.getChannel();
        if (!channel.isActive()) {
            log.warn("channel is inactive");
            return;
        }
        try {
            channel.writeAndFlush(message).sync();
        } catch (InterruptedException e) {
            throw new InterruptedException("send message fail." + e);
        }
    }


}
