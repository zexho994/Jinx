package remoting;

import consumer.ConsumerManager;
import enums.ClientType;
import enums.MessageResponseCode;
import enums.MessageType;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.log4j.Log4j2;
import message.Message;
import message.ProducerMessageResponse;
import message.PropertiesKeys;
import message.RegisterConsumer;
import netty.protocal.RemotingCommand;
import netty.server.NettyServerHandler;
import producer.ProducerManager;
import store.constant.FlushModel;
import store.constant.PutMessageResult;
import utils.ByteUtil;

/**
 * @author Zexho
 * @date 2021/11/18 4:20 下午
 */
@Log4j2
public class BrokerRemotingHandler extends NettyServerHandler {

    private final ConsumerManager consumerManager = ConsumerManager.getInstance();
    private final ProducerManager producerManager = ProducerManager.getInstance();

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
        RemotingCommand cmd = (RemotingCommand) msg;
        log.trace("[Broker] channel read msg");

        // 获取客户端类型
        String clientType = cmd.getProperty(PropertiesKeys.CLIENT_TYPE);
        if (clientType == null) {
            return;
        }
        ClientType clientTypeObj = ClientType.get(clientType);
        if (ClientType.Producer == clientTypeObj) {
            doProducerMessage(cmd, ctx);
        } else if (ClientType.Consumer == clientTypeObj) {
            doConsumerMessage(cmd, ctx);
        }
    }

    /**
     * 处理生产者的消息
     *
     * @param cmd 生产者发送的包
     */
    private void doProducerMessage(RemotingCommand cmd, ChannelHandlerContext ctx) {
        log.trace("do producer message");
        MessageType messageType = MessageType.get(cmd.getProperty(PropertiesKeys.MESSAGE_TYPE));
        Message message = ByteUtil.to(cmd.getBody(), Message.class);
        if (messageType == MessageType.Put_Message) {
            PutMessageResult putMessageResult = producerManager.putMessage(message, FlushModel.SYNC);
            ProducerMessageResponse resp = new ProducerMessageResponse();
            resp.setTransactionId(message.getTransactionId());
            switch (putMessageResult) {
                case OK:
                    resp.setCode(MessageResponseCode.SUCCESS.code);
                    break;
                case FAILURE:
                    resp.setCode(MessageResponseCode.FAILURE.code);
                    break;
                default:
            }
            ctx.writeAndFlush(resp);
        }
    }

    /**
     * 处理消费者的消息
     * 消费者的消息主要是 pull or push
     *
     * @param cmd 消费者发送的消息
     */
    private void doConsumerMessage(RemotingCommand cmd, ChannelHandlerContext ctx) {
        String messageType = cmd.getProperty(PropertiesKeys.MESSAGE_TYPE);
        switch (MessageType.get(messageType)) {
            case Register_Consumer:
                this.consumerManager.registerConsumer(ByteUtil.to(cmd.getBody(), RegisterConsumer.class), ctx);
                break;
            case Pull_Message:
            default:
        }
    }

}
