package netty.client;

import lombok.Data;

/**
 * @author Zexho
 * @date 2021/9/26 6:02 下午
 */
@Data
public class NettyClientConfig {

    /**
     * 监听端口
     */
    private int listenPort = 9944;
    /**
     * 客户端工作线程数
     */
    private final int clientWorkerThreads = 4;
    /**
     * 连接超时
     */
    private final int connectTimeoutMillis = 3000;
    /**
     * 发送缓存
     */
    private final int clientSocketSndBufSize = 65535;
    /**
     * 接受缓存
     */
    private final int clientSocketRcvBufSize = 65535;

    /**
     *
     */
    private String listenHost;

    public NettyClientConfig(String host) {
        this.listenHost = host;
    }

    public NettyClientConfig(String host, int listenPort) {
        this.listenHost = host;
        this.listenPort = listenPort;
    }
}
