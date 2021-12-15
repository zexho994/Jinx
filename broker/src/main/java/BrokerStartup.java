import lombok.extern.log4j.Log4j2;
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

    public static void main(String[] args) {
        // 初始化任务
        if (!systemInit()) {
            log.error("Failed to SystemInit");
            System.exit(-1);
        }

        // 文件恢复
        if (!mappedFileRecover()) {
            log.error("MappedFile recover fail.");
            System.exit(-1);
        }

        // 启动broker
        BrokerRemotingService brokerRemotingService = new BrokerRemotingService();
        brokerRemotingService.start();
    }

    /**
     * 初始化配置
     *
     * @return 初始化结果，true 成功
     */
    public static boolean systemInit() {
        // 初始化 Commitlog
        if (!COMMITLOG.init()) {
            log.error("Failed to init commitlog");
            return false;
        }

        // 初始化 ConsumerQueue
        if (!CONSUME_QUEUE.init()) {
            log.error("Failed to init consumeQueue");
            return false;
        }

        return true;
    }

    /**
     * 文件恢复
     *
     * @return 恢复结果 true正常, false 发生异常
     */
    public static boolean mappedFileRecover() {
        try {
            COMMITLOG.recover();
        } catch (IOException e) {
            log.error("Commitlog recover fail. ", e);
            return false;
        }

        try {
            CONSUME_QUEUE.recover();
        } catch (Exception e) {
            log.error("ConsumeQueue recover fail. ", e);
            return false;
        }

        return true;
    }
}
