package remoting;

import common.Host;
import netty.client.NettyClientConfig;
import netty.client.NettyClientHandler;
import netty.client.NettyRemotingClientImpl;
import netty.protocal.RemotingCommand;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Zexho
 * @date 2021/12/28 3:57 PM
 */
public class BrokerRemoteManager {

    private final Map<String, NettyRemotingClientImpl> brokerRemoteMap;

    public BrokerRemoteManager() {
        this.brokerRemoteMap = new ConcurrentHashMap<>();
    }

    public void connect(String brokerName, String brokerHost, NettyClientHandler handler) {
        NettyRemotingClientImpl client = new NettyRemotingClientImpl(new NettyClientConfig(brokerHost, Host.BROKER_PORT));
        if (handler != null) {
            client.setClientHandler(handler);
        }
        client.start();
        this.brokerRemoteMap.put(brokerName, client);
    }

    /**
     * 检查连接状态
     *
     * @param brokerName broker名称
     * @return true 连接可用，false连接目前有异常
     */
    public boolean checkConnectStatus(String brokerName) {
        // 检查是否建立过连接
        if (!this.brokerRemoteMap.containsKey(brokerName)) {
            return false;
        }
        // 检查连接状态是否正常
        if (!this.brokerRemoteMap.get(brokerName).getChannel().isActive()) {
            return false;
        }
        return true;
    }

    /**
     * 发送消息
     *
     * @param brokerName      要发送到的broker名称
     * @param remotingCommand 请求包
     */
    public void send(String brokerName, RemotingCommand remotingCommand) {
        NettyRemotingClientImpl broker = this.brokerRemoteMap.get(brokerName);
        broker.send(remotingCommand);
    }

}
