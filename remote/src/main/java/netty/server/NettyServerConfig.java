package netty.server;

import lombok.Data;

/**
 * @author Zexho
 * @date 2021/9/26 4:49 下午
 */
@Data
public class NettyServerConfig {
    private int listenPort = 9944;
    private int serverWorkerThreads = 8;
    private int serverSelectorThreads = 3;
}
