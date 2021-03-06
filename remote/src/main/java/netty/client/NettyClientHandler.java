package netty.client;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.log4j.Log4j2;
import netty.future.SyncFuture;
import netty.protocal.RemotingCommand;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * @author Zexho
 * @date 2021/9/26 6:14 下午
 */
@ChannelHandler.Sharable
@Log4j2
public class NettyClientHandler extends ChannelInboundHandlerAdapter {
    protected NettyRemotingClientImpl client;

    public NettyClientHandler(NettyRemotingClientImpl client) {
        this.client = client;
    }

    public NettyClientHandler() {

    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("客户端连接成功");
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("客户端断开连接");
        if (this.client != null) {
            log.info("自动重新连接...");
            ctx.channel().eventLoop().schedule(client::start, 3L, TimeUnit.SECONDS);
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.info("客户端异常 -> " + Arrays.toString(cause.getStackTrace()));
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        RemotingCommand response = (RemotingCommand) msg;
        // 如果该请求是通过 sendSync() 发送的，需要设置resp
        if (response.getBody() == null) {
            throw new Exception("response body is null, resp = " + response);
        }
        String msgId = response.getBody().getMsgId();
        if (client.syncFutureMap.containsKey(msgId)) {
            SyncFuture<RemotingCommand> remotingCommandSyncFuture = this.client.syncFutureMap.get(msgId);
            remotingCommandSyncFuture.setResponse(response);
            this.client.syncFutureMap.remove(msgId);
        }
    }
}
