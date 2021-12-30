package consumer;

import common.Host;
import message.TopicRouteInfos;
import netty.common.RemotingCommandFactory;
import remoting.BrokerRemoteManager;
import remoting.NamesrvServiceImpl;
import remoting.RemotingService;

/**
 * @author Zexho
 * @date 2021/11/15 7:33 下午
 */
public class Consumer implements RemotingService {

    private ConsumerListener consumerListener;
    private final NamesrvServiceImpl namesrvService;
    private final BrokerRemoteManager brokerRemoteManager;

    private String topic;
    private String consumerGroup;
    private final int cid;

    public Consumer(String host, int cid) {
        this.namesrvService = new NamesrvServiceImpl(host, Host.NAMESERVER_PORT);
        this.brokerRemoteManager = new BrokerRemoteManager();
        this.cid = cid;
    }

    @Override
    public void start() {
        if (this.topic == null || this.consumerGroup == null) {
            throw new RuntimeException("topic or group is null");
        }

        // 与 namesrv 连接
        this.namesrvService.start();
        // 获取topic路由信息
        TopicRouteInfos topicRouteInfo = this.namesrvService.getTopicRouteInfo(this.topic);
        // 和broker建立连接,并发送注册消息
        topicRouteInfo.getData().forEach(tf -> {
            this.brokerRemoteManager.connect(tf.getBrokerName(), tf.getBrokerHost(), new ConsumerHandler(this.consumerListener));
            this.brokerRemoteManager.send(tf.getBrokerName(), RemotingCommandFactory.registerConsumer(cid, this.topic, this.consumerGroup));
        });
    }

    @Override
    public void shutdown() {
        this.namesrvService.shutdown();
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public void setConsumerListener(ConsumerListener consumerListener) {
        this.consumerListener = consumerListener;
    }
}
