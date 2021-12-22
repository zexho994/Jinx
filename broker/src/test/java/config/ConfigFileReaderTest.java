package config;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

class ConfigFileReaderTest {

    ConfigFileReader configFileReader = new ConfigFileReader();

    @Test
    void readLocalConfigFile() {
        try {
            BrokerConfig brokerConfig = configFileReader.readBrokerConfigFile();
            List<TopicConfig> topics = brokerConfig.getTopics();
            topics.forEach(System.out::println);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}