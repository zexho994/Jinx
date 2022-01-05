package producer;

import enums.MessageType;
import lombok.extern.log4j.Log4j2;
import message.Message;
import message.TopicRouteInfo;
import netty.common.RemotingCommandFactory;
import netty.protocal.RemotingCommand;

/**
 * @author Zexho
 * @date 2021/11/15 7:35 下午
 */
@Log4j2
public class DefaultMQProducer extends Producer {

    /**
     * @param host namesrv 服务的域名
     */
    public DefaultMQProducer(String host) {
        super(host);
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
    @Override
    public void sendMessage(Message message) {
        // 获取路由信息
        TopicRouteInfo tf = namesrvService.getTopicRouteInfo(message.getTopic());
        // 检查与broker的连接
        ensureBrokerConnected(tf);
        // 随机选择一个发送队列
        if (message.getQueueId() == null) {
            message.setQueueId((int) (System.currentTimeMillis() % tf.getQueueNum()) + 1);
        }

        RemotingCommand command = RemotingCommandFactory.putMessage(message);
        this.brokerRemoteManager.send(tf.getBrokerName(), command);
    }

}
