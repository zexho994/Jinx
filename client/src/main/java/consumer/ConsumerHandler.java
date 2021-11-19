package consumer;

import Message.Message;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.log4j.Log4j2;
import netty.client.NettyClientHandler;

/**
 * @author Zexho
 * @date 2021/11/16 2:59 下午
 */
@Log4j2
public class ConsumerHandler extends NettyClientHandler {

    private final ConsumerListener consumerListener;

    public ConsumerHandler(ConsumerListener consumerListener) {
        this.consumerListener = consumerListener;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        this.consumerListener.consume((Message) msg);
    }
}
