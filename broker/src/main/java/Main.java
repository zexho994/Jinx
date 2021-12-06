import lombok.extern.log4j.Log4j2;
import remoting.BrokerRemotingService;
import store.Commitlog;
import store.ConsumeOffset;
import store.ConsumeQueue;

/**
 * @author Zexho
 * @date 2021/11/18 4:16 下午
 */
@Log4j2
public class Main {
    public static void main(String[] args) {
        // 初始化任务
        if (!systemInit()) {
            log.error("Failed to SystemInit");
            System.exit(-1);
        }

        // 启动broker
        BrokerRemotingService brokerRemotingService = new BrokerRemotingService();
        brokerRemotingService.start();
    }

    public static boolean systemInit() {
        // 初始化 Commitlog
        if (!Commitlog.getInstance().init()) {
            log.error("Failed to init commitlog");
            return false;
        }

        // 初始化 ConsumerQueue
        if (!ConsumeQueue.getInstance().init()) {
            log.error("Failed to init consumeQueue");
            return false;
        }

        if (!ConsumeOffset.getInstance().init()) {
            log.error("Failed to init consumeOffset");
            return false;
        }

        return true;
    }
}
