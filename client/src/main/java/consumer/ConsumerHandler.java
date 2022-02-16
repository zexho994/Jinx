package consumer;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.log4j.Log4j2;
import message.Message;
import netty.client.NettyClientHandler;
import netty.protocal.RemotingCommand;

/**
 * @author Zexho
 * @date 2021/11/16 2:59 下午
 */
@Log4j2
public class ConsumerHandler extends NettyClientHandler {

    private final ConsumerListener consumerListener;

    public ConsumerHandler(ConsumerListener consumerListener) {
        super();
        this.consumerListener = consumerListener;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        RemotingCommand command = (RemotingCommand) msg;
        Message message = command.getBody();
        log.debug("channel read msg => {}", message);
        this.consumerListener.consume(message);
    }
}
