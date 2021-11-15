package netty;

/**
 * @author Zexho
 * @date 2021/9/26 7:34 下午
 */
public interface IRemotingService {

    /**
     * 启动服务
     */
    void start();

    /**
     * 停止服务
     */
    void shutdown();
}
