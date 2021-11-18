import remoting.BrokerRemotingService;

/**
 * @author Zexho
 * @date 2021/11/18 4:16 下午
 */
public class Main {
    public static void main(String[] args) {
        BrokerRemotingService brokerRemotingService = new BrokerRemotingService();
        brokerRemotingService.start();
    }
}
