package remote;

import manager.BrokerManager;
import manager.TopicManager;
import enums.MessageType;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.log4j.Log4j2;
import message.ConfigBody;
import message.PropertiesKeys;
import message.TopicUnit;
import netty.protocal.RemotingCommand;
import netty.server.NettyServerHandler;
import utils.ByteUtil;

import java.util.List;

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
        log.info("[NameServer] remoting command => {}", msg);
        this.processCommand(cmd);
    }

    private void processCommand(RemotingCommand command) {
        MessageType messageType = MessageType.get(command.getProperty(PropertiesKeys.MESSAGE_TYPE));
        assert messageType != null;
        switch (messageType) {
            case Register_Broker:
                this.doRegisterBroker(command);
                break;
            default:
                break;
        }
    }

    private void doRegisterBroker(RemotingCommand command) {
        String brokerName = command.getProperty(PropertiesKeys.BROKER_NAME);
        String brokerHost = command.getProperty(PropertiesKeys.BROKER_HOST);
        String clusterName = command.getProperty(PropertiesKeys.CLUSTER_NAME);
        brokerManager.addBroker(brokerName, brokerHost, clusterName);

        ConfigBody body = ByteUtil.to(command.getBody(), ConfigBody.class);
        List<TopicUnit> topics = body.getTopics();
        topicManager.addTopic(brokerName,topics);
    }

}