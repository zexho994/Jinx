import lombok.extern.log4j.Log4j2;
import remoting.BrokerRemotingService;
import store.Commitlog;

import java.io.IOException;

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
        try {
            // Commit文件初始化
            Commitlog.Instance.init();
        } catch (IOException e) {
            log.error("Failed to init commitlog", e);
        }

    }
}
