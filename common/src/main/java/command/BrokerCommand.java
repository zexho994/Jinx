package command;

import com.beust.jcommander.Parameter;
import lombok.Data;

/**
 * @author Zexho
 * @date 2021/12/15 4:53 PM
 */
@Data
public class BrokerCommand {

    @Parameter(names = {"-namesrv", "-N"}, description = "nameserver服务的共有ip地址。如果为集群，传入所有的ip地址")
    private String namesrvHost;

    @Parameter(names = {"-broker", "-B"}, description = "broker服务的公有ip地址")
    private String brokerHost;

    @Parameter(names = {"-port", "-p"}, description = "broker的监听端口")
    private Integer brokerPort;

    @Parameter(names = {"-name", "-n"}, description = "broker实例名称")
    private String brokerName;

    @Parameter(names = {"-brokerConfigPath", "-bcp"}, description = "broker配置文件路径")
    private String brokerConfigPath;

    @Parameter(names = {"-storeConfigPath", "-scp"}, description = "store配置文件路径")
    private String storeConfigPath;
}
