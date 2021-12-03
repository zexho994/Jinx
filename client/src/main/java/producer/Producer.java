package producer;

import Message.Message;
import Message.PropertiesKeys;
import enums.ClientType;
import enums.MessageType;
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
    private final String group;

    public Producer(String group, String host) {
        this.group = group;
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

    @Override
    public void sendMessage(Message message) {
        message.setConsumerGroup(this.group);
        try {
            this.sendMessageSync(message);
        } catch (InterruptedException e) {
            log.warn("send message error. message = {} \n {}", message, e);
        }
    }

    private void sendMessageSync(Message message) throws InterruptedException {
        log.info("SEND MESSAGE => {}", message);
        message.addProperties(PropertiesKeys.CLIENT_TYPE, ClientType.Producer.type);
        message.addProperties(PropertiesKeys.MESSAGE_TYPE, MessageType.Put_Message.type);

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
