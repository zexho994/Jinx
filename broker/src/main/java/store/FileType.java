package store;

import common.MemoryCapacity;

import java.io.File;

/**
 * @author Zexho
 * @date 2021/12/3 3:04 下午
 */
public enum FileType {

    /**
     * {@link Commitlog}
     */
    COMMITLOG(System.getProperty("user.home") + File.separator + "jinx" + File.separator + "commitlog" + File.separator, MemoryCapacity.GB),

    /**
     * {@link ConsumeQueue}
     */
    CONSUME_QUEUE(System.getProperty("user.home") + File.separator + "jinx" + File.separator + "consumeQueue" + File.separator, MemoryCapacity.KB),

    /**
     * {@link ConsumeOffset}
     */
    CONSUME_OFFSET(System.getProperty("user.home") + File.separator + "jinx" + File.separator + "consumeOffset" + File.separator, MemoryCapacity.KB),
    ;

    final String basePath;
    final int fileSize;

    FileType(final String basePath, final int fileSize) {
        this.basePath = basePath;
        this.fileSize = fileSize;
    }
}
