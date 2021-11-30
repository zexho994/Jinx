import lombok.extern.log4j.Log4j2;
import remoting.BrokerRemotingService;
import store.Commitlog;

/**
 * @author Zexho
 * @date 2021/11/18 4:16 下午
 */
@Log4j2
public class Main {
    public static void main(String[] args) {
        // 初始化任务
        systemInit();

        // 启动broker
        BrokerRemotingService brokerRemotingService = new BrokerRemotingService();
        brokerRemotingService.start();
    }

    public static void systemInit() {
        if (!Commitlog.Instance.init()) {
            log.error("Failed to init commitlog");
            System.exit(0);
        }
    }
}
