package ha;

import lombok.extern.log4j.Log4j2;
import model.BrokerData;
import netty.client.NettyClientConfig;
import netty.client.NettyRemotingClientImpl;
import netty.common.RemotingCommandFactory;
import netty.protocal.RemotingCommand;
import store.DefaultMessageStore;
import store.commitlog.Commitlog;

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

    private final DefaultMessageStore messageStore = DefaultMessageStore.getInstance();
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
            NettyClientConfig config = new NettyClientConfig(masterData.getBrokerHost(), masterData.getBrokerPort());
            client = new NettyRemotingClientImpl(config);
            client.start();
        }
        if (commitlog.haveCommitlog()) {
            log.debug("slave have local commitlog");
            long commitlogOffset = commitlog.getFileFormOffset();
            log.debug("commitlog offset = {}", commitlogOffset);
        } else {
            log.debug("slave not have local commitlog");

        }

        //

        // 发送report
        RemotingCommand reportCommand = RemotingCommandFactory.slaveReportOffset();
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

}
