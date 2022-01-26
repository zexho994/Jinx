package ha;

import config.BrokerConfig;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.log4j.Log4j2;
import message.Message;
import message.PropertiesKeys;
import netty.protocal.RemotingCommand;
import netty.server.NettyRemotingServerImpl;
import netty.server.NettyServerConfig;
import netty.server.NettyServerHandler;
import store.commitlog.Commitlog;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static enums.MessageType.Get_Commitlog_Max_Offset;
import static enums.MessageType.Report_Offset;

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

    private NettyRemotingServerImpl server;

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
            String messageType = reqCommand.getProperty(PropertiesKeys.MESSAGE_TYPE);
            Message reqMessage = reqCommand.getBody();
            log.info("New slave message. type = {}, message = {}", messageType, reqMessage);

            RemotingCommand respCommand = new RemotingCommand();
            Message respMessage = new Message(reqMessage.getMsgId());
            if (Get_Commitlog_Max_Offset.type.equals(messageType)) {
                respCommand.addProperties(PropertiesKeys.MESSAGE_TYPE, Get_Commitlog_Max_Offset.type);
                long offset = this.doGetCommitlogMaxOffset();
                respMessage.setBody(offset);
            } else if (Report_Offset.type.equals(messageType)) {
                respCommand.addProperties(PropertiesKeys.MESSAGE_TYPE, Report_Offset.type);
                respMessage.setBody(this.doReportOffset(reqMessage));
            } else {
                log.warn("");
            }
            respCommand.setBody(respMessage);
            ctx.writeAndFlush(respCommand);
        }

        private List<Message> doReportOffset(Message req) {
            long slaveOffset = (long) req.getBody();
            long masterOffset = Commitlog.getInstance().getFileFormOffset();
            log.info("slave commitlog offset = {}, master commitlog offset = {}", slaveOffset, masterOffset);
            // 获取未同步的数据
            try {
                return Commitlog.getInstance().getMessageByOffset(slaveOffset);
            } catch (IOException e) {
                log.error("do report offset error.", e);
                return Collections.emptyList();
            }
        }

        private long doGetCommitlogMaxOffset() {
            return Commitlog.getInstance().getFileFormOffset();
        }

    }


}
