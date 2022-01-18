package config;

import utils.Json;

import java.io.FileReader;
import java.io.IOException;

/**
 * @author Zexho
 * @date 2021/12/16 7:44 PM
 */
public class ConfigFileReader {

    public static BrokerConfigFile readBrokerConfigFile() throws IOException {
        FileReader fr = new FileReader(BrokerConfig.brokerConfigPath);
        int i;
        StringBuilder str = new StringBuilder();
        while ((i = fr.read()) != -1) {
            str.append((char) i);
        }
        fr.close();
        return Json.fromJson(str.toString(), BrokerConfigFile.class);
    }

    public static StoreConfigFile readStoreConfigFile() throws IOException {
        FileReader fr = new FileReader(BrokerConfig.storeConfigPath);
        int i;
        StringBuilder str = new StringBuilder();
        while ((i = fr.read()) != -1) {
            str.append((char) i);
        }
        fr.close();
        return Json.fromJson(str.toString(), StoreConfigFile.class);
    }

}
