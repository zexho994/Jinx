package netty.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import lombok.extern.log4j.Log4j2;
import netty.IRemotingService;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Zexho
 * @date 2021/9/26 6:02 下午
 */
@Log4j2
public class NettyRemotingClientImpl implements IRemotingService {

    private final NettyClientConfig clientConfig;
    private final EventLoopGroup eventLoopGroupWorker;
    private ChannelFuture future;
    private NettyClientHandler clientHandler;

    public NettyRemotingClientImpl(final NettyClientConfig config) {
        this.clientConfig = config;
        this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
            private final AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("PushClient_%d", this.threadIndex.incrementAndGet()));
            }
        });
    }

    @Override
    public void start() {
        if (this.clientHandler == null) {
            this.clientHandler = new NettyClientHandler(this);
        }

        Bootstrap bootstrap = createBootstrap();
        this.future = bootstrap.connect().addListener(new ConnectionListener(this));
        log.info("客户端启动成功");
    }

    private Bootstrap createBootstrap() {
        return new Bootstrap()
                .group(this.eventLoopGroupWorker)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .remoteAddress(clientConfig.getListenHost(), clientConfig.getListenPort())
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) {
                        ch.pipeline().addLast(
                                new ObjectDecoder(ClassResolvers.cacheDisabled(this.getClass().getClassLoader())),
                                new ObjectEncoder(),
                                clientHandler);
                    }
                });
    }

    @Override
    public void shutdown() {
        this.eventLoopGroupWorker.shutdownGracefully();
    }
}
