package remoting;

import common.Host;
import consumer.ConsumerHandler;
import lombok.extern.log4j.Log4j2;
import netty.client.NettyClientConfig;
import netty.client.NettyRemotingClientImpl;
import netty.protocal.RemotingCommand;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * @author Zexho
 * @date 2021/12/28 3:57 PM
 */
@Log4j2
public class BrokerRemoteManager {

    private final Map<String, NettyRemotingClientImpl> brokerRemoteMap;

    public BrokerRemoteManager() {
        this.brokerRemoteMap = new ConcurrentHashMap<>();
    }

    /**
     * 建立与broker的连接
     *
     * @param brokerName broker名称，用于保存路由信息
     * @param brokerHost broker网络地址，用于建立连接
     * @param handler    如果需要自定义处理器
     */
    public void connect(String brokerName, String brokerHost, ConsumerHandler handler) {
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
        log.debug("send message. brokerName = {}. command = {}", brokerName, remotingCommand);

        NettyRemotingClientImpl broker = this.brokerRemoteMap.get(brokerName);
        broker.send(remotingCommand);
    }

    /**
     * 同步发送消息
     *
     * @param brokerName      broker名称
     * @param remotingCommand 要发送的消息包
     * @return 请求返回体
     */
    public RemotingCommand sendSync(String brokerName, RemotingCommand remotingCommand) throws ExecutionException, InterruptedException {
        log.debug("sync send message. brokerName = {}. command = {}", brokerName, remotingCommand);

        NettyRemotingClientImpl broker = this.brokerRemoteMap.get(brokerName);
        return broker.sendSync(remotingCommand);
    }

}
