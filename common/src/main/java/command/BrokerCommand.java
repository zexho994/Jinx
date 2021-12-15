package command;

import com.beust.jcommander.Parameter;
import lombok.Data;

/**
 * @author Zexho
 * @date 2021/12/15 4:53 PM
 */
@Data
public class BrokerCommand {

    @Parameter(names = {"-namesrv", "-N"}, description = "nameserver服务的ip地址。如果为集群，传入所有的ip地址", required = false)
    private String namesrvHost;

}
