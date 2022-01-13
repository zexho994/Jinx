package remoting;

import enums.MessageType;
import message.PropertiesKeys;
import message.TopicRouteInfo;
import message.TopicRouteInfos;
import netty.client.NettyClientConfig;
import netty.client.NettyRemotingClientImpl;
import netty.protocal.RemotingCommand;

import java.util.concurrent.ExecutionException;

/**
 * @author Zexho
 * @date 2021/12/27 4:15 PM
 */
public class NamesrvServiceImpl implements RemotingService {

    private final NettyRemotingClientImpl client;

    public NamesrvServiceImpl(String namesrvHost, int namesrvPort) {
        NettyClientConfig nettyClientConfig = new NettyClientConfig(namesrvHost, namesrvPort);
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
     * 获取topic所有的路由信息
     *
     * @param topic topic 名称
     * @return 路由信息对象 {@link TopicRouteInfos}
     */
    public TopicRouteInfos getTopicRouteInfos(String topic) {
        RemotingCommand command = new RemotingCommand();
        command.addProperties(PropertiesKeys.MESSAGE_TYPE, MessageType.Get_Topic_Route.type);
        command.addProperties(PropertiesKeys.TOPIC, topic);
        try {
            RemotingCommand resp = this.client.sendSync(command);
            return (TopicRouteInfos) resp.getBody().getBody();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取topic的某个路由信息
     *
     * @param topic topic 名称
     * @return 路由信息对象
     */
    public TopicRouteInfo getTopicRouteInfo(String topic) {
        // 获取路由信息
        TopicRouteInfos topicRouteInfo = this.getTopicRouteInfos(topic);
        // 随机选择一个发送队列
        int size = topicRouteInfo.getData().size();
        return topicRouteInfo.getData().get((int) (System.currentTimeMillis() % size));
    }
}
