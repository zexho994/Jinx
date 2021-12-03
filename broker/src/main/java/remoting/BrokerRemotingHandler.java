package remoting;

import Message.Message;
import Message.PropertiesKeys;
import enums.ClientType;
import enums.MessageType;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.log4j.Log4j2;
import netty.server.NettyServerHandler;
import queue.MessageManager;
import store.FlushModel;

/**
 * @author Zexho
 * @date 2021/11/18 4:20 下午
 */
@Log4j2
public class BrokerRemotingHandler extends NettyServerHandler {

    private final MessageManager messageManager = MessageManager.getInstance();

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

        // 获取客户端类型
        String clientType = message.getProperty(PropertiesKeys.CLIENT_TYPE);
        if (clientType == null) {
            return;
        }
        ClientType clientTypeObj = ClientType.get(clientType);
        if (ClientType.Producer == clientTypeObj) {
            doProducerMessage(message);
        } else if (ClientType.Consumer == clientTypeObj) {
            doConsumerMessage(message, ctx);
        }
    }

    /**
     * 处理生产者的消息
     *
     * @param message 生产者发送的消息
     */
    public void doProducerMessage(Message message) {
        MessageType messageType = MessageType.get(message.getProperty(PropertiesKeys.MESSAGE_TYPE));
        if (messageType == MessageType.Put_Message) {
            messageManager.putMessage(message, FlushModel.SYNC);
        }
    }

    /**
     * 处理消费者的消息
     * 消费者的消息主要是 pull or push
     *
     * @param message 消费者发送的消息
     */
    public void doConsumerMessage(Message message, ChannelHandlerContext ctx) {
        String topic = message.getTopic();
        Message pullMessage = messageManager.pullMessage(topic);
        if (pullMessage != null) {
            ctx.writeAndFlush(pullMessage);
        }
    }

}
