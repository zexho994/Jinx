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
    private final int listenPort = 9944;
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
     * 监听url，根据不同环境进行修改
     * 开发: push-netty.server-dev.test.maxhub.vip
     * 测试: push-netty.server.test.maxhub.vip
     * 生产: join.maxhub.com/netty.server/ps/v2
     */
    private String listenHost;
    /**
     * 环境
     */
    private int env;

    public NettyClientConfig(String host) {
        this.listenHost = host;
    }
}
