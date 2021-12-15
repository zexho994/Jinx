import com.beust.jcommander.JCommander;
import command.BrokerCommand;
import lombok.extern.log4j.Log4j2;
import netty.client.NettyClientConfig;
import remoting.BrokerNamesrvService;
import remoting.BrokerRemotingService;
import store.commitlog.Commitlog;
import store.consumequeue.ConsumeQueue;

import java.io.IOException;

/**
 * @author Zexho
 * @date 2021/11/18 4:16 下午
 */
@Log4j2
public class BrokerStartup {

    private static final Commitlog COMMITLOG = Commitlog.getInstance();
    private static final ConsumeQueue CONSUME_QUEUE = ConsumeQueue.getInstance();
    private static String nameSrvHost;

    public static void main(String[] args) throws Exception {
        // 启动参数解析
        try {
            parseCommander(args);
        } catch (Exception e) {
            throw new Exception("parse commander error.", e);
        }

        // 系统初始化
        try {
            systemInit();
        } catch (Exception e) {
            throw new Exception("systemInit error.", e);
        }

        // 恢复文件
        try {
            mappedFileRecover();
        } catch (IOException e) {
            throw new Exception("mappedFile recover error.", e);
        }

        // 启动broker
        BrokerRemotingService brokerRemotingService = new BrokerRemotingService();
        brokerRemotingService.start();

        // namesrv 连接服务
        NettyClientConfig nettyClientConfig = new NettyClientConfig(nameSrvHost);
        nettyClientConfig.setListenPort(9876);
        BrokerNamesrvService brokerNamesrvService = new BrokerNamesrvService(nettyClientConfig);
        brokerNamesrvService.start();
    }

    /**
     * 解析启动命令参数
     *
     * @param args 启动参数
     */
    public static void parseCommander(String[] args) throws Exception {
        BrokerCommand startCommand = new BrokerCommand();
        JCommander jCommander = JCommander.newBuilder().addObject(startCommand).build();
        jCommander.parse(args);

        nameSrvHost = startCommand.getNamesrvHost();
        if (nameSrvHost == null) {
            throw new Exception("nameserver host cannot be null");
        }
    }

    /**
     * 初始化配置
     */
    public static void systemInit() throws Exception {
        // 初始化 Commitlog
        try {
            COMMITLOG.init();
        } catch (Exception e) {
            throw new Exception("Failed init commitlog");
        }

        // 初始化 ConsumerQueue
        try {
            CONSUME_QUEUE.init();
        } catch (Exception e) {
            throw new Exception("Failed init consumeQueue");
        }
    }

    /**
     * 文件恢复
     *
     * @return 恢复结果 true正常, false 发生异常
     */
    public static void mappedFileRecover() throws Exception {
        try {
            COMMITLOG.recover();
        } catch (IOException e) {
            throw new Exception("commitlog recover error. ", e);
        }

        try {
            CONSUME_QUEUE.recover();
        } catch (Exception e) {
            throw new Exception("ConsumeQueue recover fail. ", e);
        }
    }
}
