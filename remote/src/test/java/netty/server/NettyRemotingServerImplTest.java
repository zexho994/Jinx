package netty.server;

import org.junit.jupiter.api.Test;

class NettyRemotingServerImplTest {

    public static void main(String[] args) {
        NettyRemotingServerImpl server = new NettyRemotingServerImpl(new NettyServerConfig(), new NettyServerHandler());
        server.start();
    }

    @Test
    void start() {
        NettyRemotingServerImpl server = new NettyRemotingServerImpl(new NettyServerConfig(), new NettyServerHandler());
        server.start();
    }

    @Test
    void shutdown() {
    }
}