package config;

import config.file.BrokerInfo;
import store.constant.FileType;
import utils.Json;

import java.io.FileReader;
import java.io.IOException;

/**
 * @author Zexho
 * @date 2021/12/16 7:44 PM
 */
public class ConfigFileReader {

    public BrokerInfo readLocalConfigFile() throws IOException {
        FileReader fr = new FileReader(FileType.BROKER_CONFIG.basePath);
        int i;
        StringBuilder str = new StringBuilder();
        while ((i = fr.read()) != -1) {
            str.append((char) i);
        }
        fr.close();
        return Json.fromJson(str.toString(), BrokerInfo.class);
    }

}
