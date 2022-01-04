package producer;

import common.Host;
import message.Message;
import message.TopicRouteInfos;
import remoting.BrokerRemoteManager;
import remoting.NamesrvServiceImpl;
import remoting.RemotingService;

/**
 * @author Zexho
 * @date 2022/1/4 5:34 PM
 */
public abstract class Producer implements RemotingService {

    protected final NamesrvServiceImpl namesrvService;
    protected final BrokerRemoteManager brokerRemoteManager;

    public Producer(String host) {
        this.namesrvService = new NamesrvServiceImpl(host, Host.NAMESERVER_PORT);
        this.brokerRemoteManager = new BrokerRemoteManager();
    }

    @Override
    public void start() {
        this.namesrvService.start();
    }

    @Override
    public void shutdown() {
        this.namesrvService.shutdown();
    }

    /**
     * 发送消息
     *
     * @param message 要发送的消息对象
     */
    public abstract void sendMessage(Message message) throws Exception;

    /**
     * 与topic路由信息中的所有broker客户端进行连接
     * 已连接的不在重复连接
     *
     * @param topicRouteInfo 路由信息
     */
    protected void ensureBrokerConnected(TopicRouteInfos topicRouteInfo) {
        topicRouteInfo.getData().stream()
                .filter(tf -> !brokerRemoteManager.checkConnectStatus(tf.getBrokerName()))
                .forEach(tf -> this.brokerRemoteManager.connect(tf.getBrokerName(), tf.getBrokerHost(), null));
    }

}
