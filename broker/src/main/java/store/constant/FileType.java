package store.constant;

import common.MemoryCapacity;
import store.commitlog.Commitlog;
import store.consumequeue.ConsumeQueue;

import java.io.File;

/**
 * @author Zexho
 * @date 2021/12/3 3:04 下午
 */
public enum FileType {

    /**
     * {@link Commitlog}
     */
    COMMITLOG(System.getProperty("user.home") + File.separator + "jinx" + File.separator + "commitlog" + File.separator, 64 * MemoryCapacity.KB),

    /**
     * {@link ConsumeQueue}
     */
    CONSUME_QUEUE(System.getProperty("user.home") + File.separator + "jinx" + File.separator + "consumeQueue" + File.separator,MemoryCapacity.KB / 10),

    CONSUME_OFFSET(System.getProperty("user.home") + File.separator + "jinx" + File.separator + "consumeOffset" + File.separator, MemoryCapacity.B * 8),
    ;

    public final String basePath;
    public final int fileSize;

    FileType(final String basePath, final int fileSize) {
        this.basePath = basePath;
        this.fileSize = fileSize;
    }
}
