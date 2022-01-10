package remote;

import enums.MessageType;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.log4j.Log4j2;
import manager.BrokerManager;
import manager.TopicManager;
import message.*;
import meta.BrokerData;
import netty.protocal.RemotingCommand;
import netty.server.NettyServerHandler;
import utils.ByteUtil;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * @author Zexho
 * @date 2021/12/15 3:59 PM
 */
@Log4j2
public class NameSrvRemotingHandler extends NettyServerHandler {

    private final BrokerManager brokerManager = BrokerManager.getInstance();
    private final TopicManager topicManager = TopicManager.getInstance();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        RemotingCommand cmd = (RemotingCommand) msg;
        RemotingCommand response = this.processCommand(cmd);
        ctx.writeAndFlush(response);
    }

    private RemotingCommand processCommand(RemotingCommand command) {
        log.debug("nameserver process command. cmd = {}", command);
        MessageType messageType = MessageType.get(command.getProperty(PropertiesKeys.MESSAGE_TYPE));
        assert messageType != null;
        Message reqMessage = ByteUtil.toMessage(command.getBody());
        RemotingCommand resp = new RemotingCommand();
        switch (messageType) {
            case Register_Broker:
                log.info("[NameServer] broker register => {}", ByteUtil.to(command.getBody(), Message.class));
                boolean result = this.doRegisterBroker(command);
                resp.addProperties(PropertiesKeys.MESSAGE_TYPE, MessageType.Register_Broker_Resp.type);
                resp.setBody(ByteUtil.to(result));
                break;
            case Get_Topic_Route:
                String topic = command.getProperty(PropertiesKeys.TOPIC);
                log.info("[NameServer] get topic route info => {}", topic);
                TopicRouteInfos topicRouteInfos = this.doGetTopicRouteData(topic);
                resp.addProperties(PropertiesKeys.MESSAGE_TYPE, MessageType.Get_Topic_Route.type);
                Message body = new Message(reqMessage.getMsgId());
                body.setBody(topicRouteInfos);
                resp.setBody(ByteUtil.to(body));
                break;
            default:
                break;
        }
        return resp;
    }

    private boolean doRegisterBroker(RemotingCommand command) {
        String brokerName = command.getProperty(PropertiesKeys.BROKER_NAME);
        String brokerHost = command.getProperty(PropertiesKeys.BROKER_HOST);
        String clusterName = command.getProperty(PropertiesKeys.CLUSTER_NAME);
        brokerManager.addBroker(brokerName, brokerHost, clusterName);

        Message message = ByteUtil.toMessage(command.getBody());
        ConfigBody body = (ConfigBody) message.getBody();
        List<TopicUnit> topics = body.getTopics();
        topicManager.addTopic(brokerName, topics);

        return true;
    }

    private TopicRouteInfos doGetTopicRouteData(String topic) {
        log.info("Get topic <{}> route data", topic);
        Set<String> brokerNames = this.topicManager.getBrokerNameByTopic(topic);
        List<TopicRouteInfo> topicRouteInfoList = new LinkedList<>();

        brokerNames.forEach(e -> {
            BrokerData brokerData = this.brokerManager.getBrokerData(e);
            this.topicManager.getTopicsByBrokerName(e).stream()
                    .filter(tu -> tu.getTopic().equals(topic))
                    .forEach(tu -> {
                        TopicRouteInfo tri = new TopicRouteInfo();
                        tri.setBrokerName(brokerData.getBrokerName());
                        tri.setClusterName(brokerData.getClusterName());
                        tri.setBrokerHost(brokerData.getBrokerHost());
                        tri.setTopic(tu.getTopic());
                        tri.setQueueNum(tu.getQueue());
                        topicRouteInfoList.add(tri);
                    });
        });
        return new TopicRouteInfos(topicRouteInfoList);
    }

}
