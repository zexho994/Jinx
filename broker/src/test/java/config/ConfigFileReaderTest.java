package config;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

class ConfigFileReaderTest {

    ConfigFileReader configFileReader = new ConfigFileReader();

    @Test
    void readLocalConfigFile() {
        try {
            BrokerConfigFile brokerConfig = ConfigFileReader.readBrokerConfigFile();
            System.out.println(brokerConfig);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}