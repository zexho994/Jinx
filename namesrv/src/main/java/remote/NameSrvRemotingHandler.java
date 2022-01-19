package remote;

import enums.MessageType;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.log4j.Log4j2;
import manager.BrokerManager;
import manager.TopicManager;
import message.*;
import model.BrokerData;
import netty.protocal.RemotingCommand;
import netty.server.NettyServerHandler;
import utils.Broker;

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
        RemotingCommand resp = new RemotingCommand();
        Message respMessage = new Message(command.getBody().getMsgId());
        switch (messageType) {
            case Register_Broker:
                log.info("[NameServer] broker register => {}", command.getBody());
                // 注册broker信息
                this.doRegisterBroker(command);
                // 返回信息
                int brokerId = Integer.parseInt(command.getProperty(PropertiesKeys.BROKER_ID));
                if (Broker.isMaster(brokerId)) {
                    respMessage.setBody(true);
                } else {
                    // broker为slave，返回master的路由信息
                    String clusterName = command.getProperty(PropertiesKeys.CLUSTER_NAME);
                    String brokerName = command.getProperty(PropertiesKeys.BROKER_NAME);
                    BrokerData masterData = brokerManager.getMasterData(clusterName, brokerName);
                    respMessage.setBody(masterData);
                }
                resp.addProperties(PropertiesKeys.MESSAGE_TYPE, MessageType.Register_Broker_Resp.type);
                resp.setBody(respMessage);
                break;
            case Get_Topic_Route:
                String topic = command.getProperty(PropertiesKeys.TOPIC);
                log.info("[NameServer] get topic route info => {}", topic);
                TopicRouteInfos topicRouteInfos = this.doGetTopicRouteData(topic);
                resp.addProperties(PropertiesKeys.MESSAGE_TYPE, MessageType.Get_Topic_Route.type);
                respMessage.setBody(topicRouteInfos);
                resp.setBody(respMessage);
                break;
            default:
                throw new RuntimeException("message type error: " + messageType);
        }
        return resp;
    }

    private void doRegisterBroker(RemotingCommand command) {
        String clusterName = command.getProperty(PropertiesKeys.CLUSTER_NAME);
        String brokerName = command.getProperty(PropertiesKeys.BROKER_NAME);
        String brokerHost = command.getProperty(PropertiesKeys.BROKER_HOST);
        int brokerId = Integer.parseInt(command.getProperty(PropertiesKeys.BROKER_ID));
        int brokerPort = Integer.parseInt(command.getProperty(PropertiesKeys.BROKER_PORT));
        brokerManager.addBroker(brokerName, brokerHost, brokerPort, brokerId, clusterName);

        // master要保存topic的消息,slave不用
        if (Broker.isMaster(brokerId)) {
            Message message = command.getBody();
            ConfigBody body = (ConfigBody) message.getBody();
            List<TopicUnit> topics = body.getTopics();
            topicManager.addTopic(brokerName, topics);
        }
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
                        tri.setBrokerPort(brokerData.getBrokerPort());
                        tri.setBrokerId(brokerData.getBrokerId());
                        tri.setTopic(tu.getTopic());
                        tri.setQueueNum(tu.getQueue());
                        topicRouteInfoList.add(tri);
                    });
        });
        return new TopicRouteInfos(topicRouteInfoList);
    }

}
