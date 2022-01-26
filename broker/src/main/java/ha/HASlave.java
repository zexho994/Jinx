package ha;

import config.BrokerConfig;
import enums.MessageType;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.log4j.Log4j2;
import message.Message;
import message.PropertiesKeys;
import model.BrokerData;
import netty.client.NettyClientConfig;
import netty.client.NettyClientHandler;
import netty.client.NettyRemotingClientImpl;
import netty.common.RemotingCommandFactory;
import netty.protocal.RemotingCommand;
import store.commitlog.Commitlog;
import store.constant.FlushModel;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Zexho
 * @date 2022/1/18 5:31 PM
 */
@Log4j2
public class HASlave {

    private HASlave() {
    }

    private static class Inner {
        private static final HASlave INSTANCE = new HASlave();
    }

    public static HASlave getInstance() {
        return HASlave.Inner.INSTANCE;
    }

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private BrokerData masterData = null;
    private NettyRemotingClientImpl client;
    private final Commitlog commitlog = Commitlog.getInstance();

    /**
     * 向master汇报同步进度（即commitlog offset）
     */
    public void startReportOffset() {
        scheduledExecutorService.scheduleAtFixedRate(this::reportTask, 0, 5, TimeUnit.SECONDS);
    }

    private void reportTask() {
        if (this.masterData == null) {
            return;
        }
        if (client == null) {
            NettyClientConfig config = new NettyClientConfig(masterData.getBrokerHost(), BrokerConfig.MASTER_LISTER_PORT);
            client = new NettyRemotingClientImpl(config);
            client.setClientHandler(new HASlaveHandler(client));
            client.start();
        }
        long commitlogOffset = commitlog.getCommitlogOffset();
        log.debug("commitlog offset = {}", commitlogOffset);

        RemotingCommand reportCommand = RemotingCommandFactory.slaveReportOffset(commitlogOffset);
        client.send(reportCommand);
    }

    /**
     * 保存master的路由信息，用于建立连接
     *
     * @param masterData master的路由信息
     */
    public void saveMasterRouteData(BrokerData masterData) {
        log.info("save master route data");
        this.masterData = masterData;
    }

    @Log4j2
    static class HASlaveHandler extends NettyClientHandler {

        public HASlaveHandler(NettyRemotingClientImpl client) {
            super(client);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            RemotingCommand command = (RemotingCommand) msg;
            log.info(" HASlave received command => {}", command);
            String messageType = command.getProperty(PropertiesKeys.MESSAGE_TYPE);
            if (!Objects.equals(messageType, MessageType.Report_Offset.type)) {
                throw new RuntimeException("message type error");
            }
            Message message = command.getBody();
            List<Message> messages = (List<Message>) message.getBody();
            if (messages.size() == 0) {
                return;
            }
            for (Message m : messages) {
                Commitlog.getInstance().putMessage(m, FlushModel.SYNC);
            }
        }
    }

}
