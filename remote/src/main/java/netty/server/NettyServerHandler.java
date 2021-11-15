package netty.server;

import Message.Message;
import Message.MessageType;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.log4j.Log4j2;

/**
 * @author Zexho
 * @date 2021/9/26 3:48 下午
 */
@ChannelHandler.Sharable
@Log4j2
public class NettyServerHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        Message ack = new Message(MessageType.SUCCESS);
        ctx.channel().writeAndFlush(ack);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("新的客户端连接");
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("捕捉异常:" + cause.getMessage());
    }

}
