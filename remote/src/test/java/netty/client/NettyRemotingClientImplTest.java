package netty.client;

import org.junit.jupiter.api.Test;

public class NettyRemotingClientImplTest {

    public static void main(String[] args) {
        NettyRemotingClientImpl nettyRemotingClient = new NettyRemotingClientImpl(new NettyClientConfig("127.0.0.1"));
        nettyRemotingClient.start();
    }

    @Test
    public void testStart() {
        NettyRemotingClientImpl nettyRemotingClient = new NettyRemotingClientImpl(new NettyClientConfig("127.0.0.1"));
        nettyRemotingClient.start();
    }

    @Test
    void testShutdown() {
    }
}