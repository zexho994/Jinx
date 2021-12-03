package store;

import Message.Message;
import common.MemoryCapacity;
import lombok.extern.log4j.Log4j2;
import utils.Json;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Zexho
 * @date 2021/12/2 10:30 上午
 */
@Log4j2
public class ConsumeQueue {

    private final Lock lock = new ReentrantLock();

    private ConsumeQueue() {
    }

    private static class Inner {
        private static final ConsumeQueue INSTANCE = new ConsumeQueue();
    }

    public static ConsumeQueue getInstance() {
        return Inner.INSTANCE;
    }

    public static File CONSUMER_QUEUE_FOLDER;
    private final Map<String, MappedFile> mappedFileMap = new ConcurrentHashMap<>();
    private final int DEFAULT_CONSUME_QUEUE_FILE_SIZE = MemoryCapacity.GB;

    public boolean init() {
        CONSUMER_QUEUE_FOLDER = new File(FileType.CONSUME_QUEUE.basePath);
        try {
            this.ensureDirExist();
        } catch (Exception e) {
            log.error("ConsumeQueue init error", e);
            return false;
        }
        return true;
    }

    private void ensureDirExist() throws Exception {
        if (CONSUMER_QUEUE_FOLDER == null) {
            throw new Exception("CONSUMER_QUEUE_FOLDER is null");
        }

        if (!CONSUMER_QUEUE_FOLDER.exists()) {
            CONSUMER_QUEUE_FOLDER.mkdirs();
        }
    }

    /**
     * 存储消息到 ConsumeQueue 文件中
     *
     * @param message
     * @param offset
     * @param msgSize
     */
    public PutMessageResult putMessage(Message message, int offset, int msgSize) {
        String topic = message.getTopic();
        lock.lock();

        try {
            MappedFile mappedFile = ensureFileExist(topic);
            ConsumeQueueData consumeQueueData = new ConsumeQueueData(offset, msgSize);
            byte[] data = Json.toJsonLine(consumeQueueData).getBytes();
            mappedFile.append(data);
            mappedFile.flush();
            return PutMessageResult.OK;
        } catch (IOException e) {
            return PutMessageResult.FAILURE;
        } finally {
            lock.unlock();
        }
    }

    static class ConsumeQueueData {
        public final int offset;
        public final int size;

        public ConsumeQueueData(int offset, int size) {
            this.offset = offset;
            this.size = size;
        }
    }

    private MappedFile ensureFileExist(String topic) throws IOException {
        MappedFile mappedFile = mappedFileMap.get(topic);
        if (mappedFile == null) {
            mappedFile = new MappedFile(FileType.CONSUME_QUEUE, topic, DEFAULT_CONSUME_QUEUE_FILE_SIZE);
            mappedFileMap.put(topic, mappedFile);
        }
        return mappedFile;

    }

}
