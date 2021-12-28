package producer;

import common.Host;
import enums.MessageType;
import lombok.extern.log4j.Log4j2;
import message.Message;
import message.TopicRouteInfo;
import message.TopicRouteInfos;
import netty.client.NettyClientConfig;
import netty.client.NettyRemotingClientImpl;
import netty.common.RemotingCommandFactory;
import netty.protocal.RemotingCommand;
import remoting.NamesrvServiceImpl;
import remoting.RemotingService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Zexho
 * @date 2021/11/15 7:35 下午
 */
@Log4j2
public class Producer implements RemotingService {

    final MessageRequestTable messageRequestTable;
    private final NamesrvServiceImpl namesrvService;
    private final Map<String, NettyRemotingClientImpl> brokerRemoteMap;


    /**
     * @param host namesrv 服务的域名
     */
    public Producer(String host) {
        this.namesrvService = new NamesrvServiceImpl(host, Host.NAMESERVER_PORT);
        this.brokerRemoteMap = new ConcurrentHashMap<>();
        this.messageRequestTable = new MessageRequestTable(this);
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
     * 发送消息到broker
     * <p>
     * step1: 从namesrv获取topic的路由信息地址
     * step2: 和broker建立netty连接
     * step3: 发送{@link MessageType#Put_Message}消息到broker
     * step4: 等待 broker 的 ack
     *
     * @param message 消息包
     */
    public void sendMessage(Message message) {
        // 获取路由信息
        TopicRouteInfos topicRouteInfo = this.namesrvService.getTopicRouteInfo(message.getTopic());
        // 检查与broker的连接
        this.checkBrokerConnected(topicRouteInfo);
        // 消息包
        RemotingCommand command = RemotingCommandFactory.putMessage(message);
        // 选择一个发送队列,随机选择一个
        int size = topicRouteInfo.getData().size();
        TopicRouteInfo tf = topicRouteInfo.getData().get((int) (System.currentTimeMillis() % size));
        this.brokerRemoteMap.get(tf.getBrokerName()).send(command);
    }


    public void setAfterRetryProcess(AfterRetryProcess afterRetryProcess) {
        ProducerConfig.afterRetryProcess = afterRetryProcess;
    }

    /**
     * 与topic路由信息中的所有broker客户端进行连接
     * 已连接的不在重复连接
     *
     * @param topicRouteInfo 路由信息
     */
    private void checkBrokerConnected(TopicRouteInfos topicRouteInfo) {
        topicRouteInfo.getData().stream()
                .filter(tf -> !this.brokerRemoteMap.containsKey(tf.getBrokerName()))
                .forEach(tf -> {
                    NettyClientConfig config = new NettyClientConfig(tf.getBrokerHost(), Host.BROKER_PORT);
                    NettyRemotingClientImpl client = new NettyRemotingClientImpl(config);
                    client.start();
                    this.brokerRemoteMap.put(tf.getBrokerName(), client);
                });
    }

}
