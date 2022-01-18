package config;

import lombok.Data;

/**
 * @author Zexho
 * @date 2022/1/18 3:50 PM
 */
@Data
public class StoreConfigFile {
    private String commitlogPath;
    private Integer commitlogSize;

    private String consumeQueuePath;
    private Integer consumeQueueSize;

    private String consumeOffsetPath;
    private Integer consumeOffsetSize;
}
