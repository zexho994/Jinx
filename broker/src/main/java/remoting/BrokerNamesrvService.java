package remoting;

import enums.ClientType;
import enums.MessageType;
import io.netty.channel.Channel;
import message.Message;
import message.PropertiesKeys;
import netty.client.NettyClientConfig;
import netty.client.NettyRemotingClientImpl;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * @author Zexho
 * @date 2021/12/15 8:02 PM
 */
public class BrokerNamesrvService {

    private final NettyRemotingClientImpl client;
    private final NettyClientConfig nettyClientConfig;
    private Channel channel;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private final Message heartbeat;
    private static final int namesrvPort = 9876;

    public BrokerNamesrvService(NettyClientConfig nettyClientConfig) {
        this.nettyClientConfig = nettyClientConfig;
        this.client = new NettyRemotingClientImpl(nettyClientConfig);
        this.heartbeat = new Message();
        heartbeat.addProperties(PropertiesKeys.CLIENT_TYPE, ClientType.Broker.type);
        heartbeat.addProperties(PropertiesKeys.MESSAGE_TYPE, MessageType.Heart_Beat.type);
    }

    public void start() {
        this.client.start();
        this.channel = this.client.getChannel();
        this.scheduledExecutorService.scheduleAtFixedRate(this::heartbeat, 5000, 30000, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        this.client.start();
    }

    public void heartbeat() {
        this.channel.writeAndFlush(heartbeat);
    }

}
