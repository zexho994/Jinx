package producer;

import enums.ClientType;
import enums.MessageType;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.extern.log4j.Log4j2;
import message.Message;
import message.PropertiesKeys;
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
    final MessageRequestTable messageRequestTable;
    AfterRetryProcess afterRetryProcess;

    public Producer(String group, String host) {
        this.group = group;
        NettyClientConfig config = new NettyClientConfig(host);
        ProducerHandler handler = new ProducerHandler(this);
        this.nettyRemotingClient = new NettyRemotingClientImpl(config, handler);
        this.messageRequestTable = new MessageRequestTable(this);
    }

    @Override
    public void start() {
        this.nettyRemotingClient.start();
        this.messageRequestTable.start();
    }

    @Override
    public void shutdown() {
        this.nettyRemotingClient.shutdown();
    }

    @Override
    public void sendMessage(Message message) {
        message.setConsumerGroup(this.group);
        try {
            // 消息保存到发送队列中
            this.messageRequestTable.offer(message);
            // 执行发送
            this.sendMessageSync(message);
        } catch (InterruptedException e) {
            log.warn("send message error. message = {} \n {}", message, e);
        }
    }

    /**
     * 同步发送消息
     *
     * @param message 要发送的消息
     * @throws InterruptedException
     */
    void sendMessageSync(Message message) throws InterruptedException {
        log.info("SEND MESSAGE => {}", message);
        message.addProperties(PropertiesKeys.CLIENT_TYPE, ClientType.Producer.type);
        message.addProperties(PropertiesKeys.MESSAGE_TYPE, MessageType.Put_Message.type);

        Channel channel = this.nettyRemotingClient.getChannel();
        if (!channel.isActive()) {
            log.warn("channel is inactive");
            return;
        }
        try {
            ChannelFuture sync = channel.writeAndFlush(message).sync();
            if (!sync.isSuccess()) {
                log.warn("Send message fail,transactionId = {}", message.getTransactionId());
                messageRequestTable.retryRequest(message.getTransactionId());
            }
        } catch (InterruptedException e) {
            throw new InterruptedException("send message fail." + e);
        }
    }

    public void setAfterRetryProcess(AfterRetryProcess afterRetryProcess) {
        this.afterRetryProcess = afterRetryProcess;
    }
}
