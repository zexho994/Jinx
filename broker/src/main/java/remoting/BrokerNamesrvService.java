package remoting;

import config.BrokerConfig;
import enums.ClientType;
import enums.MessageType;
import lombok.extern.log4j.Log4j2;
import message.Message;
import message.PropertiesKeys;
import netty.client.NettyClientConfig;
import netty.client.NettyRemotingClientImpl;
import netty.protocal.RemotingCommand;
import utils.ByteUtil;

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

    public RemotingCommand getHeartbeatCommand() {
        RemotingCommand heartbeat = new RemotingCommand();
        heartbeat.addProperties(PropertiesKeys.CLIENT_TYPE, ClientType.Broker.type);
        heartbeat.addProperties(PropertiesKeys.MESSAGE_TYPE, MessageType.Register_Broker.type);
        heartbeat.addProperties(PropertiesKeys.BROKER_HOST, BrokerConfig.brokerHost);
        heartbeat.addProperties(PropertiesKeys.BROKER_NAME, BrokerConfig.brokerName);
        heartbeat.addProperties(PropertiesKeys.CLUSTER_NAME, BrokerConfig.clusterName);
        Message message = new Message();
        message.setBody(BrokerConfig.configBody);
        heartbeat.setBody(message);
        return heartbeat;
    }

    public void start() {
        this.client.start();
        this.scheduledExecutorService.scheduleAtFixedRate(this::heartbeat, 5000, 30000, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        this.client.start();
    }

    public void heartbeat() {
        try {
            RemotingCommand resp = this.client.sendSync(this.getHeartbeatCommand());
            if (resp.getProperty(PropertiesKeys.MESSAGE_TYPE).equals(MessageType.Register_Broker_Resp.type)) {
                Boolean res = ByteUtil.to(resp.getBody(), Boolean.class);
                if (res) {
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
