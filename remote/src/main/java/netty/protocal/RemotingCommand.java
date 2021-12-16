package netty.protocal;

import lombok.Data;
import message.Message;
import utils.ByteUtil;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Zexho
 * @date 2021/12/16 10:15 AM
 */
@Data
public class RemotingCommand implements Serializable {
    private static final long serialVersionUID = 6798972145219873378L;

    private Map<String, String> properties = new ConcurrentHashMap<>();

    private byte[] message;

    public void addProperties(String key, String val) {
        properties.put(key, val);
    }

    public String getProperty(String key) {
        return properties.get(key);
    }

    public void setMessage(byte[] message) {
        this.message = message;
    }

    public byte[] getMessage() {
        return this.message;
    }

    @Override
    public String toString() {
        return "RemotingCommand{" +
                "properties=" + properties +
                ", message=" + Arrays.toString(message) +
                '}';
    }
}
