import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author Zexho
 * @date 2021/12/31 2:47 PM
 */
public class ClusterTest {

    String namesrvHost = "127.0.0.1";

    /**
     * consumer集群消费测试
     * 1. 所有 consumer实例 同一group，同一topic
     * 2.
     */
    @Test
    public void consumerClusterTest() throws InterruptedException {
        String topic = "topic_1";
        String group = "cluster_group";
        Set<String> msgSet = new CopyOnWriteArraySet<>();

        //step1: 启动consumer集群
        ConsumerTest.startConsumer(topic, group, 1, msgSet);
        ConsumerTest.startConsumer(topic, group, 2, msgSet);
        ConsumerTest.startConsumer(topic, group, 3, msgSet);

        //step2: producer 发送消息
        ProducerTest.produceMessage(topic, 10, msgSet);
    }


}
