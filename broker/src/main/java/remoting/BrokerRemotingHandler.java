package remoting;

import Message.Message;
import Message.PropertiesKeys;
import enums.ClientType;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.log4j.Log4j2;
import netty.server.NettyServerHandler;

/**
 * @author Zexho
 * @date 2021/11/18 4:20 下午
 */
@Log4j2
public class BrokerRemotingHandler extends NettyServerHandler {

    /**
     * 有新的producer或者consumer接入
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.info("NEW CLIENT CONNECTION");
    }

    /**
     * 接收到新的request
     *
     * @param ctx
     * @param msg
     * @throws Exception
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Message message = (Message) msg;
        log.info("receive request => {}", message);

        String clientType = message.getProperty(PropertiesKeys.CLIENT_TYPE);
        if (clientType == null) {
            return;
        }

        ClientType clientTypeObj = ClientType.get(clientType);
        if (ClientType.Producer == clientTypeObj) {
            doProducerMessage(message);
        } else if (ClientType.Consumer == clientTypeObj) {
            doConsumerMessage(message);
        }
    }

    /**
     * 处理生产者的消息
     *
     * @param message 生产者发送的消息
     */
    public void doProducerMessage(Message message) {
        log.info("process producer's message");
    }

    /**
     * 处理消费者的消息
     *
     * @param message 消费者发送的消息
     */
    public void doConsumerMessage(Message message) {
        log.info("process consumer's message");
    }

}
