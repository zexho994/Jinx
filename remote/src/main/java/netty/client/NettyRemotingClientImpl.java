package netty.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import lombok.extern.log4j.Log4j2;
import message.Message;
import netty.IRemotingService;
import netty.future.SyncFuture;
import netty.protocal.RemotingCommand;
import utils.ByteUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
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
    private Channel channel;
    private NettyClientHandler clientHandler;
    public final Map<String, SyncFuture<RemotingCommand>> syncFutureMap = new ConcurrentHashMap<>();

    public NettyRemotingClientImpl(final NettyClientConfig config) {
        this(config, null);
    }

    public NettyRemotingClientImpl(final NettyClientConfig config, NettyClientHandler handler) {
        this.clientConfig = config;
        this.clientHandler = handler;
        this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
            private final AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyClientThread_%d", this.threadIndex.incrementAndGet()));
            }
        });
    }

    @Override
    public void start() {
        if (this.clientHandler == null) {
            this.clientHandler = new NettyClientHandler(this);
        }

        Bootstrap bootstrap = createBootstrap();
        try {
            ChannelFuture sync = bootstrap.connect().sync();
            this.channel = sync.addListener(new ConnectionListener(this)).channel();
        } catch (InterruptedException e) {
            log.warn("启动失败");
        }
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

    /**
     * 异步发送消息
     *
     * @param command 请求体
     */
    public void send(RemotingCommand command) {
        this.channel.writeAndFlush(command);
    }

    /**
     * 同步发送消息
     *
     * @param command 请求体
     * @return 响应结果
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public RemotingCommand sendSync(RemotingCommand command) throws ExecutionException, InterruptedException {
        SyncFuture<RemotingCommand> future = new SyncFuture<>();
        if (command.getBody() == null) {
            command.setBody(new Message());
        }
        syncFutureMap.put(ByteUtil.toMessage(command.getBody()).getMsgId(), future);
        this.channel.writeAndFlush(command);
        return future.get();
    }

    @Override
    public void shutdown() {
        this.eventLoopGroupWorker.shutdownGracefully();
    }

    public Channel getChannel() {
        return this.channel;
    }

    public void setClientHandler(NettyClientHandler clientHandler) {
        this.clientHandler = clientHandler;
    }
}
