package store.constant;

import config.StoreConfig;

/**
 * @author Zexho
 * @date 2021/12/3 3:04 下午
 */
public enum FileType {
    /**
     * commitlog 文件, {@link store.commitlog.Commitlog}
     */
    COMMITLOG,

    /**
     * consumeQueue 文件,{@link store.consumequeue.ConsumeQueue}
     */
    CONSUME_QUEUE,

    /**
     * consumeOffset 文件,{@link store.consumequeue.ConsumeOffset}
     */
    CONSUME_OFFSET;

    public static String getFilePath(FileType type) {
        if (type == COMMITLOG) {
            return StoreConfig.commitlogPath;
        }
        if (type == CONSUME_OFFSET) {
            return StoreConfig.consumeOffsetPath;
        }
        if (type == CONSUME_QUEUE) {
            return StoreConfig.consumeQueuePath;
        }
        throw new RuntimeException("file type error");
    }

    public static int getFileSize(FileType type) {
        if (type == COMMITLOG) {
            return StoreConfig.commitlogSize;
        }
        if (type == CONSUME_OFFSET) {
            return StoreConfig.consumeOffsetSize;
        }
        if (type == CONSUME_QUEUE) {
            return StoreConfig.consumeQueueSize;
        }
        throw new RuntimeException("file type error");
    }
}
