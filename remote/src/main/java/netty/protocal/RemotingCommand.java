package netty.protocal;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Zexho
 * @date 2021/12/16 10:15 AM
 */
@Data
@ToString
public class RemotingCommand implements Serializable {
    private static final long serialVersionUID = 6798972145219873378L;

    private String traceId;

    private Map<String, String> properties = new ConcurrentHashMap<>();

    private byte[] body;

    public RemotingCommand() {
        this.traceId = UUID.randomUUID().toString();
    }

    public RemotingCommand(String traceId) {
        this.traceId = traceId;
    }

    public void addProperties(String key, String val) {
        properties.put(key, val);
    }

    public String getProperty(String key) {
        return properties.get(key);
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public byte[] getBody() {
        return this.body;
    }

}
