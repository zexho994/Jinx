package netty.client;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.log4j.Log4j2;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * @author Zexho
 * @date 2021/9/26 6:14 下午
 */
@ChannelHandler.Sharable
@Log4j2
public class NettyClientHandler extends ChannelInboundHandlerAdapter {
    protected final NettyRemotingClientImpl client;

    public NettyClientHandler(NettyRemotingClientImpl client) {
        this.client = client;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("客户端连接成功");
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("客户端断开连接");
        ctx.channel().eventLoop().schedule(client::start, 3L, TimeUnit.SECONDS);
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.info("客户端异常 -> " + Arrays.toString(cause.getStackTrace()));
    }

}
