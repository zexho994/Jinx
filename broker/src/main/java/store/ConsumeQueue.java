package store;

import Message.Message;
import lombok.extern.log4j.Log4j2;
import utils.Json;

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
        private final Stack<MappedFile> fileQueue;

        public ConsumeQueueFiles(MappedFile mappedFile) {
            this.fileQueue = new Stack<>();
            fileQueue.add(mappedFile);
        }

        public MappedFile getLastFile() {
            return fileQueue.peek();
        }

        /**
         * 添加一个新文件
         */
        public void createNewFile() {
            MappedFile lastFile = this.getLastFile();
            String fileName = lastFile.getFileName();
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
     * @param message
     * @param offset
     * @param msgSize
     */
    public PutMessageResult putMessage(Message message, int offset, int msgSize) {
        String topic = message.getTopic();
        lock.lock();
        try {
            ensureFileExist(topic);
            ConsumeQueueFiles consumeQueueFiles = mappedFileMap.get(topic);
            MappedFile mappedFile = consumeQueueFiles.getLastFile();

            ConsumeQueueData consumeQueueData = new ConsumeQueueData(offset, msgSize);
            byte[] data = Json.toJsonLine(consumeQueueData).getBytes();

            MessageAppendResult appendResult = mappedFile.append(data);
            if (MessageAppendResult.OK == appendResult) {
                mappedFile.flush();
            } else if (MessageAppendResult.INSUFFICIENT_SPACE == appendResult) {
                log.info("ConsumeQueue INSUFFICIENT_SPACE");
                consumeQueueFiles.createNewFile();
                mappedFile = consumeQueueFiles.getLastFile();
                mappedFile.append(data);
                mappedFile.flush();
            }

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
        ConsumeQueueFiles consumeQueueFiles = new ConsumeQueueFiles(mappedFile);

        this.mappedFileMap.put(topic, consumeQueueFiles);
    }

}
