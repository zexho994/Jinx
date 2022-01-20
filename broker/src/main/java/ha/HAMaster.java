package ha;

import config.BrokerConfig;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.log4j.Log4j2;
import message.Message;
import netty.protocal.RemotingCommand;
import netty.server.NettyRemotingServerImpl;
import netty.server.NettyServerConfig;
import netty.server.NettyServerHandler;

/**
 * @author Zexho
 * @date 2022/1/18 5:30 PM
 */
public class HAMaster {

    private HAMaster() {
    }

    private static class Inner {
        private static final HAMaster INSTANCE = new HAMaster();
    }

    public static HAMaster getInstance() {
        return HAMaster.Inner.INSTANCE;
    }

    NettyRemotingServerImpl server;

    /**
     * 监听slave的连接
     */
    public void startListenSlave() {
        NettyServerConfig config = new NettyServerConfig();
        config.setListenPort(BrokerConfig.MASTER_LISTER_PORT);
        server = new NettyRemotingServerImpl(config);
        server.setServerHandler(new HAMasterHandler());
        server.start();
    }

    @Log4j2
    static class HAMasterHandler extends NettyServerHandler {

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            log.info("New slave connection");
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            RemotingCommand reqCommand = (RemotingCommand) msg;
            log.info("New slave message => {}", reqCommand);
            Message reqMessage = reqCommand.getBody();



            Message respMessage = new Message(reqMessage.getMsgId());
            RemotingCommand respCommand = new RemotingCommand();
            respCommand.setBody(respMessage);
            ctx.writeAndFlush(respCommand);
        }

    }


}
