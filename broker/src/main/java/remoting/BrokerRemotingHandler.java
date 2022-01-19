package remoting;

import client.ConsumerManager;
import enums.ClientType;
import enums.MessageType;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.log4j.Log4j2;
import message.Message;
import message.PropertiesKeys;
import message.RegisterConsumer;
import netty.common.RemotingCommandFactory;
import netty.protocal.RemotingCommand;
import netty.server.NettyServerHandler;
import client.ProducerManager;
import store.constant.FlushModel;
import store.constant.PutMessageResult;

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
        log.debug("read remoting command => {}", cmd);

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
        MessageType messageType = MessageType.get(cmd.getProperty(PropertiesKeys.MESSAGE_TYPE));
        if (messageType == MessageType.Put_Message) {
            PutMessageResult putMessageResult;
            Message message = cmd.getBody();
            if (isTransactionMessage(cmd)) {
                log.trace("Processing transaction messages");
                putMessageResult = producerManager.transactionMessageProcessor(cmd, FlushModel.SYNC);
            } else {
                log.trace("Processing ordinary messages");
                putMessageResult = producerManager.messageProcessor(message, FlushModel.SYNC);
            }
            RemotingCommand resp = RemotingCommandFactory.putMessageResp(message, putMessageResult.code);
            ctx.writeAndFlush(resp);
        }
    }

    /**
     * 判断是否是事务消息
     */
    private boolean isTransactionMessage(RemotingCommand command) {
        return command.getProperty(PropertiesKeys.TRAN) != null;
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
                Message body = cmd.getBody();
                RegisterConsumer registerConsumer = (RegisterConsumer) body.getBody();
                this.consumerManager.registerConsumer(registerConsumer, ctx);
                break;
            case Pull_Message:
            default:
        }
    }

}
