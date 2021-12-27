package remoting;

import message.TopicRouteInfos;
import org.junit.jupiter.api.Test;

class NamesrvServiceTest {

    @Test
    void getRouteInfo() {
        NamesrvServiceImpl namesrvService = new NamesrvServiceImpl("127.0.0.1", 9876);
        namesrvService.start();

        TopicRouteInfos topicRouteInfo = namesrvService.getTopicRouteInfo("topic_1");
        topicRouteInfo.getData().forEach(System.out::println);
    }
}