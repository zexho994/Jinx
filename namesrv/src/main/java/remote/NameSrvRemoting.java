package remote;

import common.Host;
import netty.server.NettyRemotingServerImpl;
import netty.server.NettyServerConfig;

/**
 * @author Zexho
 * @date 2021/12/15 3:57 PM
 */
public class NameSrvRemoting {

    private final NettyRemotingServerImpl remotingServer;

    public NameSrvRemoting() {
        NettyServerConfig config = new NettyServerConfig();
        config.setListenPort(Host.NAMESERVER_PORT);
        this.remotingServer = new NettyRemotingServerImpl(config);
        this.remotingServer.setServerHandler(new NameSrvRemotingHandler());
    }

    public void start() {
        this.remotingServer.start();
    }

}
