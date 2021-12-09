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
public class Main {

    private static final Commitlog commitlog = Commitlog.getInstance();
    private static final ConsumeQueue consumeQueue = ConsumeQueue.getInstance();

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

    public static boolean systemInit() {
        // 初始化 Commitlog
        if (!commitlog.init()) {
            log.error("Failed to init commitlog");
            return false;
        }

        // 初始化 ConsumerQueue
        if (!consumeQueue.init()) {
            log.error("Failed to init consumeQueue");
            return false;
        }

        return true;
    }

    public static boolean mappedFileRecover() {
        try {
            commitlog.recover();
        } catch (IOException e) {
            log.error("Commitlog recover fail. ", e);
            return false;
        }

        try {
            consumeQueue.recover();
        } catch (Exception e) {
            log.error("ConsumeQueue recover fail. ", e);
            return false;
        }

        return true;
    }
}
