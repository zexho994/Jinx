package remote;

import common.Host;
import lombok.extern.log4j.Log4j2;
import netty.server.NettyRemotingServerImpl;
import netty.server.NettyServerConfig;

/**
 * @author Zexho
 * @date 2021/12/15 3:57 PM
 */
@Log4j2
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
        log.info("namesrv started successfully, listening to port 9876");
    }

}
