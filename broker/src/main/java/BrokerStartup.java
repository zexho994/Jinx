import com.beust.jcommander.JCommander;
import command.BrokerCommand;
import config.BrokerConfig;
import config.BrokerConfigFile;
import config.ConfigFileReader;
import consumer.ConsumerManager;
import lombok.extern.log4j.Log4j2;
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

    public static void main(String[] args) throws Exception {
        // 启动参数解析
        try {
            parseCommander(args);
        } catch (Exception e) {
            throw new Exception("parse commander error.", e);
        }

        // 配置文件加载
        try {
            parseConfig();
        } catch (Exception e) {
            throw new Exception("parse config error.", e);
        }

        // 恢复文件
        try {
            mappedFileRecover();
        } catch (IOException e) {
            throw new Exception("mappedFile recover error.", e);
        }

        // 系统初始化
        try {
            if (BrokerConfig.brokerId == 0) {
                mappedFileInit();
            }
        } catch (Exception e) {
            throw new Exception("systemInit error.", e);
        }


        // 启动broker
        BrokerRemotingService brokerRemotingService = new BrokerRemotingService();
        brokerRemotingService.start();

        // namesrv 连接服务
        BrokerNamesrvService brokerNamesrvService = new BrokerNamesrvService();
        brokerNamesrvService.start();

        // consumer push 定时器
        ConsumerManager.getInstance().startPushTask();
    }

    /**
     * 解析启动命令参数
     *
     * @param args 启动参数
     */
    private static void parseCommander(String[] args) throws Exception {
        BrokerCommand startCommand = new BrokerCommand();
        JCommander jCommander = JCommander.newBuilder().addObject(startCommand).build();
        jCommander.parse(args);

        if (startCommand.getNamesrvHost() == null) {
            throw new Exception("nameserver host cannot be null");
        }

        BrokerConfig.nameSrvHost = startCommand.getNamesrvHost();

        if (startCommand.getBrokerHost() == null) {
            throw new Exception("broker host cannot be null");
        }
        BrokerConfig.brokerHost = startCommand.getBrokerHost();

        if (startCommand.getBrokerName() != null) {
            BrokerConfig.brokerName = startCommand.getBrokerName();
        }
        if (startCommand.getBrokerConfigPath() != null) {
            BrokerConfig.brokerConfigPath = startCommand.getBrokerConfigPath();
        }
        if (startCommand.getBrokerPort() != null) {
            BrokerConfig.brokerPort = startCommand.getBrokerPort();
        }
    }

    /**
     * 文件初始化配置
     */
    private static void mappedFileInit() throws Exception {
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
     */
    private static void mappedFileRecover() throws Exception {
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

    private static void parseConfig() throws Exception {
        try {
            BrokerConfigFile brokerConfigFile = ConfigFileReader.readBrokerConfigFile();
            BrokerConfig.brokerName = brokerConfigFile.getBrokerName();
            BrokerConfig.clusterName = brokerConfigFile.getClusterName();
            BrokerConfig.brokerId = brokerConfigFile.getBrokerId();
            BrokerConfig.configBody = brokerConfigFile.getBody();
        } catch (IOException e) {
            throw new Exception("read broker config file error.", e);
        }
    }
}
