package remoting;

import lombok.extern.log4j.Log4j2;
import netty.server.NettyRemotingServerImpl;
import netty.server.NettyServerConfig;

/**
 * @author Zexho
 * @date 2021/11/18 4:17 下午
 */
@Log4j2
public class BrokerRemotingService {

    private final NettyRemotingServerImpl nettyRemotingClient;

    public BrokerRemotingService() {
        NettyServerConfig config = new NettyServerConfig();
        this.nettyRemotingClient = new NettyRemotingServerImpl(config);
        this.nettyRemotingClient.setServerHandler(new BrokerRemotingHandler());
    }

    public void start() {
        this.nettyRemotingClient.start();
    }

}
