package store;

import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Stack;
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
    private final Map<String, ConsumeQueueFiles> mappedFileMap = new ConcurrentHashMap<>();

    private static class ConsumeQueueFiles {

        private final String topic;
        private final Stack<MappedFile> fileQueue;

        public ConsumeQueueFiles(String topic, MappedFile mappedFile) {
            this.topic = topic;
            this.fileQueue = new Stack<>();
            fileQueue.add(mappedFile);
        }

        public MappedFile getLastFile() {
            return fileQueue.peek();
        }

        /**
         * 添加一个新文件
         */
        public void createNewFile() throws IOException {
            MappedFile lastFile = this.getLastFile();
            String fileName = lastFile.getFileName();
            int wrotePos = lastFile.getWrotePos();
            String nextFileOffset = String.valueOf(Integer.parseInt(fileName) + wrotePos + 1);
            File file = new File(CONSUMER_QUEUE_FOLDER.getAbsolutePath() + File.separator + topic + File.separator + nextFileOffset);
            MappedFile mappedFile = new MappedFile(FileType.CONSUME_QUEUE, file);
            this.fileQueue.push(mappedFile);
        }

    }

    public boolean init() {
        try {
            this.ensureDirExist();
        } catch (Exception e) {
            log.error("ConsumeQueue init error", e);
            return false;
        }
        return true;
    }

    private void ensureDirExist() {
        if (CONSUMER_QUEUE_FOLDER == null) {
            CONSUMER_QUEUE_FOLDER = new File(FileType.CONSUME_QUEUE.basePath);
            CONSUMER_QUEUE_FOLDER.mkdirs();
        }
    }

    /**
     * 存储消息到 ConsumeQueue 文件中
     *
     * @param topic  消息主题
     * @param offset 消息在commit中的偏移量,总偏移量
     */
    public PutMessageResult putMessage(String topic, long offset) {
        lock.lock();
        try {
            ensureFileExist(topic);
            ConsumeQueueFiles consumeQueueFiles = mappedFileMap.get(topic);
            MappedFile mappedFile = consumeQueueFiles.getLastFile();

            MessageAppendResult appendResult = mappedFile.appendLong(offset);
            if (MessageAppendResult.OK == appendResult) {
                mappedFile.flush();
            } else if (MessageAppendResult.INSUFFICIENT_SPACE == appendResult) {
                log.info("ConsumeQueue INSUFFICIENT_SPACE");
                consumeQueueFiles.createNewFile();
                mappedFile = consumeQueueFiles.getLastFile();
                mappedFile.appendLong(offset);
                mappedFile.flush();
            }

            return PutMessageResult.OK;
        } catch (IOException e) {
            return PutMessageResult.FAILURE;
        } finally {
            lock.unlock();
        }
    }

    public long getCommitlogOffset(String topic, int seq) throws IOException {
        ConsumeQueueFiles consumeQueueFiles = mappedFileMap.get(topic);
        return consumeQueueFiles.getLastFile().getLong(seq);
    }

    static class ConsumeQueueData {
        public final long offset;

        public ConsumeQueueData(long offset) {
            this.offset = offset;
        }
    }

    /**
     * 确保 ConsumeQueue 文件存在
     *
     * @param topic 主题名称
     * @throws IOException
     */
    private void ensureFileExist(String topic) throws IOException {
        if (mappedFileMap.containsKey(topic)) {
            return;
        }
        // 创建文件
        File topicFolder = new File(FileType.CONSUME_QUEUE.basePath + topic);
        topicFolder.mkdirs();
        File file = new File(topicFolder.getAbsolutePath() + File.separator + "0");
        file.createNewFile();

        // 记录保存到 map 中
        MappedFile mappedFile = new MappedFile(FileType.CONSUME_QUEUE, file);
        ConsumeQueueFiles consumeQueueFiles = new ConsumeQueueFiles(topic, mappedFile);

        this.mappedFileMap.put(topic, consumeQueueFiles);
    }

}
