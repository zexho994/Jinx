package netty.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import lombok.extern.log4j.Log4j2;
import netty.IRemotingService;

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Zexho
 * @date 2021/9/26 4:48 下午
 */
@Log4j2
public class NettyRemotingServerImpl implements IRemotingService {
    private final ServerBootstrap serverBootstrap;
    private final NettyServerConfig nettyServerConfig;
    private final EventLoopGroup eventLoopGroupBoss;
    private final EventLoopGroup eventLoopGroupSelector;
    private NettyServerHandler serverHandler;
    private Channel channel;

    public NettyRemotingServerImpl(NettyServerConfig config) {
        this(config, null);
    }

    public NettyRemotingServerImpl(NettyServerConfig config, NettyServerHandler handler) {
        this.serverHandler = handler;
        this.nettyServerConfig = config;
        this.serverBootstrap = new ServerBootstrap();
        this.eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactory() {
            private final AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("PushServerGroupBoos_%d", this.threadIndex.incrementAndGet()));
            }
        });
        this.eventLoopGroupSelector = new NioEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
            private final AtomicInteger threadIndex = new AtomicInteger(0);
            private final int threadTotal = nettyServerConfig.getServerSelectorThreads();

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("PushServerGroupSelector_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));
            }
        });
    }

    @Override
    public void start() {
        this.serverBootstrap
                .group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
                .channel(NioServerSocketChannel.class)
                .localAddress(new InetSocketAddress(this.nettyServerConfig.getListenPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(
                                new ObjectDecoder(ClassResolvers.cacheDisabled(this.getClass().getClassLoader())),
                                new ObjectEncoder(),
                                serverHandler
                        );
                    }
                });
        try {
            ChannelFuture sync = this.serverBootstrap.bind().sync();
            this.channel = sync.channel();
            log.info("服务端启动成功");
        } catch (InterruptedException e) {
            throw new RuntimeException("bind fail. " + e);
        }
    }

    @Override
    public void shutdown() {
        try {
            this.eventLoopGroupBoss.shutdownGracefully().sync();
            this.eventLoopGroupSelector.shutdownGracefully().sync();
        } catch (InterruptedException e) {
            throw new RuntimeException("PushServer 服务停止异常, " + e);
        }
    }

    public Channel getChannel() {
        return this.channel;
    }

    public void setServerHandler(NettyServerHandler handler) {
        this.serverHandler = handler;
    }

}
