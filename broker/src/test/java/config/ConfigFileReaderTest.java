package config;

import config.file.BrokerInfo;
import config.file.TopicInfo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

class ConfigFileReaderTest {

    ConfigFileReader configFileReader = new ConfigFileReader();

    @Test
    void readLocalConfigFile() {
        try {
            BrokerInfo brokerInfo = configFileReader.readLocalConfigFile();
            List<TopicInfo> topics = brokerInfo.getTopics();
            topics.forEach(e -> System.out.println(e.toString()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}