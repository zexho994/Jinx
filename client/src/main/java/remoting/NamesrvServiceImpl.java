package remoting;

import enums.MessageType;
import message.PropertiesKeys;
import message.TopicRouteInfos;
import netty.client.NettyClientConfig;
import netty.client.NettyRemotingClientImpl;
import netty.protocal.RemotingCommand;
import utils.ByteUtil;

import java.util.concurrent.ExecutionException;

/**
 * @author Zexho
 * @date 2021/12/27 4:15 PM
 */
public class NamesrvServiceImpl implements RemotingService {

    private final NettyRemotingClientImpl client;

    public NamesrvServiceImpl(String namesrvHost, int namesrvPort) {
        NettyClientConfig nettyClientConfig = new NettyClientConfig(namesrvHost);
        nettyClientConfig.setListenPort(namesrvPort);
        this.client = new NettyRemotingClientImpl(nettyClientConfig);
    }

    @Override
    public void start() {
        this.client.start();
    }

    @Override
    public void shutdown() {
        this.client.start();
    }

    /**
     * 获取topic的路由信息
     *
     * @param topic topic 名称
     * @return 路由信息对象 {@link TopicRouteInfos}
     */
    public TopicRouteInfos getTopicRouteInfo(String topic) {
        RemotingCommand command = new RemotingCommand();
        command.addProperties(PropertiesKeys.MESSAGE_TYPE, MessageType.Get_Topic_Route.type);
        command.setBody(ByteUtil.to(topic));
        try {
            RemotingCommand resp = this.client.sendSync(command);
            return ByteUtil.to(resp.getBody(), TopicRouteInfos.class);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }
}
