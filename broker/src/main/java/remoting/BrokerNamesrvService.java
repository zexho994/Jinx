package remoting;

import config.BrokerConfig;
import enums.MessageType;
import ha.HASlave;
import lombok.extern.log4j.Log4j2;
import message.Message;
import message.PropertiesKeys;
import model.BrokerData;
import netty.client.NettyClientConfig;
import netty.client.NettyRemotingClientImpl;
import netty.common.RemotingCommandFactory;
import netty.protocal.RemotingCommand;
import utils.Broker;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * @author Zexho
 * @date 2021/12/15 8:02 PM
 */
@Log4j2
public class BrokerNamesrvService {

    private final NettyRemotingClientImpl client;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public BrokerNamesrvService() {
        NettyClientConfig nettyClientConfig = new NettyClientConfig(BrokerConfig.brokerHost);
        nettyClientConfig.setListenPort(BrokerConfig.NAMESRV_PORT);
        this.client = new NettyRemotingClientImpl(nettyClientConfig);
    }

    public void start() {
        this.client.start();
        this.scheduledExecutorService.scheduleAtFixedRate(this::heartbeat, 3, 30, TimeUnit.SECONDS);
    }

    public void shutdown() {
        this.client.start();
    }

    public void heartbeat() {
        try {
            RemotingCommand heartbeat = RemotingCommandFactory.brokerHeartbeat(BrokerConfig.brokerHost, BrokerConfig.brokerName,
                    BrokerConfig.brokerPort, BrokerConfig.brokerId, BrokerConfig.clusterName, BrokerConfig.configBody);
            RemotingCommand resp = this.client.sendSync(heartbeat);
            Message res = resp.getBody();
            if (resp.getProperty(PropertiesKeys.MESSAGE_TYPE).equals(MessageType.Register_Broker_Resp.type)) {
                if (Broker.isMaster(BrokerConfig.brokerId)) {
                    boolean flag = (boolean) res.getBody();
                    if (flag) {
                        log.info("master register success");
                    } else {
                        log.warn("master register error");
                    }
                } else {
                    BrokerData masterData = (BrokerData) res.getBody();
                    HASlave.getInstance().saveMasterRouteData(masterData);
                }
            }
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
