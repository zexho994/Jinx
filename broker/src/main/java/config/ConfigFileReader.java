package config;

import lombok.extern.log4j.Log4j2;
import utils.Json;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * @author Zexho
 * @date 2021/12/16 7:44 PM
 */
@Log4j2
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
        File file = new File(BrokerConfig.storeConfigPath);
        if (!file.exists()) {
            log.warn("store config is not exist");
            return null;
        }

        FileReader fr = new FileReader(file);
        int i;
        StringBuilder str = new StringBuilder();
        while ((i = fr.read()) != -1) {
            str.append((char) i);
        }
        fr.close();
        return Json.fromJson(str.toString(), StoreConfigFile.class);
    }

}
