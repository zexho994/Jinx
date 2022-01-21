import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Zexho
 * @date 2021/12/31 2:47 PM
 */
public class ClusterTest {

    String namesrvHost = "127.0.0.1";
    String topic_1 = "topic_1";
    String group_1 = "cluster_group";
    String group_2 = "cluster_group_2";

    /**
     * consumer集群消费测试
     * <p>
     * 前提：
     * 1. namesrv 和 broker 应用已启动
     * 2. 所有 consumer 实例 同一group，同一topic
     * <p>
     * 期望：
     * 消息只会被集群下的 consumer 消费一次
     */
    @Test
    public void consumerClusterTest() throws InterruptedException {
        Map<String, Integer> msgMap = new ConcurrentHashMap<>();

        //step1: 启动consumer集群
        ConsumerTest.startConsumer(topic_1, group_1, 1, msgMap);
//        ConsumerTest.startConsumer(topic_1, group_1, 2, msgMap);
//        ConsumerTest.startConsumer(topic_1, group_1, 3, msgMap);

        //step2: producer 发送消息
        DefaultMQProducerTest.produceMessage(topic_1, 10, msgMap, 1);
    }

    /**
     * 不同集群的 consumer 都会消费同一条消息
     * <p>
     * 前提：
     * 2个集群，都订阅同一个 topic
     * <p>
     * 期望：
     * 一条消息会分别被两个集群消费，每个集群消费一次
     */
    @Test
    public void consumerClusterTest2() throws InterruptedException {
        Map<String, Integer> msgMap = new ConcurrentHashMap<>();
        ConsumerTest.startConsumer(topic_1, group_1, 1, msgMap);
        ConsumerTest.startConsumer(topic_1, group_1, 2, msgMap);
        ConsumerTest.startConsumer(topic_1, group_2, 4, msgMap);
        ConsumerTest.startConsumer(topic_1, group_2, 5, msgMap);

        DefaultMQProducerTest.produceMessage(topic_1, 10, msgMap, 2);
    }


}
