package netty.client;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.util.concurrent.TimeUnit;

/**
 * 连接状态监听器
 *
 * @author Zexho
 * @date 2021/10/8 5:17 下午
 */
public class ConnectionListener implements ChannelFutureListener {

    private final NettyRemotingClientImpl client;

    public ConnectionListener(NettyRemotingClientImpl client) {
        this.client = client;
    }

    @Override
    public void operationComplete(ChannelFuture future) {
        if (!future.isSuccess()) {
            future.channel().eventLoop().schedule(client::start, 3L, TimeUnit.SECONDS);
        }
    }

}
