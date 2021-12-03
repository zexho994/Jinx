package store;

import java.io.File;

/**
 * @author Zexho
 * @date 2021/12/3 3:04 下午
 */
public enum FileType {

    /**
     * {@link Commitlog}
     */
    COMMITLOG(System.getProperty("user.home") + File.separator + "jinx" + File.separator + "commitlog" + File.separator),

    /**
     * {@link ConsumeQueue}
     */
    CONSUME_QUEUE(System.getProperty("user.home") + File.separator + "jinx" + File.separator + "consumeQueue" + File.separator),
    ;

    final String basePath;

    FileType(final String basePath) {
        this.basePath = basePath;
    }
}
