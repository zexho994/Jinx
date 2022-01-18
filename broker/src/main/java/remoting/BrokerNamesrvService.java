package remoting;

import config.BrokerConfig;
import enums.MessageType;
import lombok.extern.log4j.Log4j2;
import message.Message;
import message.PropertiesKeys;
import netty.client.NettyClientConfig;
import netty.client.NettyRemotingClientImpl;
import netty.common.RemotingCommandFactory;
import netty.protocal.RemotingCommand;

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
            if (resp.getProperty(PropertiesKeys.MESSAGE_TYPE).equals(MessageType.Register_Broker_Resp.type)) {
                Message res = resp.getBody();
                boolean flag = (boolean) res.getBody();
                if (flag) {
                    log.info("broker register success");
                } else {
                    log.warn("broker register error");
                }
            }
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
