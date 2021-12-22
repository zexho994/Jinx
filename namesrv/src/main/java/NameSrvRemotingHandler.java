import enums.ClientType;
import enums.MessageType;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.log4j.Log4j2;
import message.PropertiesKeys;
import netty.protocal.RemotingCommand;
import netty.server.NettyServerHandler;

/**
 * @author Zexho
 * @date 2021/12/15 3:59 PM
 */
@Log4j2
public class NameSrvRemotingHandler extends NettyServerHandler {

    private final BrokerManager brokerManager = BrokerManager.getInstance();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        RemotingCommand cmd = (RemotingCommand) msg;
        log.info("[NameServer] command => {}", msg);

        String clientType = cmd.getProperty(PropertiesKeys.CLIENT_TYPE);
        if (!ClientType.Broker.type.equals(clientType)) {
            log.warn("client type error, tt should be <broker>, but now it's <{}>", clientType);
            return;
        }

        String messageType = cmd.getProperty(PropertiesKeys.MESSAGE_TYPE);
        if (!MessageType.Register_Broker.type.equals(messageType)) {
            log.warn("message type error, tt should be <heartbeat>, but now it's <{}>", messageType);
            return;
        }

        String brokerName = cmd.getProperty(PropertiesKeys.BROKER_NAME);
        String brokerHost = cmd.getProperty(PropertiesKeys.BROKER_HOST);
        String clusterName = cmd.getProperty(PropertiesKeys.CLUSTER_NAME);
        brokerManager.addBroker(brokerName, brokerHost, clusterName);
    }

}
