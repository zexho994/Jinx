package store.consumequeue;

import lombok.extern.log4j.Log4j2;
import store.constant.FileType;
import store.constant.MessageAppendResult;
import store.constant.PutMessageResult;
import store.mappedfile.MappedFile;

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
    private final ConsumeOffset consumeOffset = ConsumeOffset.getInstance();

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
            int fromOffset = lastFile.getFromOffset();
            int wrotePos = lastFile.getWrotePos();
            String nextFileOffset = String.valueOf(fromOffset + wrotePos);
            File file = new File(CONSUMER_QUEUE_FOLDER.getAbsolutePath() + File.separator + topic + File.separator + nextFileOffset);
            MappedFile mappedFile = new MappedFile(FileType.CONSUME_QUEUE, file);
            this.fileQueue.push(mappedFile);
        }

        /**
         * 根据偏移量找到对应存储文件
         *
         * @param offset 要查找消息的总偏移量
         * @return
         */
        public MappedFile getFileByOffset(long offset) throws Exception {
            if (offset < 0) {
                throw new Exception("offset must be positive");
            }
            for (int i = fileQueue.size() - 1; i >= 0; i--) {
                MappedFile mappedFile = fileQueue.get(i);
                if (mappedFile.getFromOffset() <= offset) {
                    return mappedFile;
                }
            }
            throw new Exception("Get file by offset fail, offset = " + offset);
        }
    }

    public boolean init() {
        try {
            this.ensureDirExist();
            this.consumeOffset.init();
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
     * @param topic           消息主题
     * @param commitlogOffset 消息在commit中的偏移量,总偏移量
     */
    public PutMessageResult putMessage(String topic, long commitlogOffset) {
        lock.lock();
        try {
            ensureFileExist(topic);

            ConsumeQueueFiles consumeQueueFiles = mappedFileMap.get(topic);
            MappedFile mappedFile = consumeQueueFiles.getLastFile();

            MessageAppendResult appendResult = mappedFile.appendLong(commitlogOffset);
            if (MessageAppendResult.OK == appendResult) {
                mappedFile.flush();
            } else if (MessageAppendResult.INSUFFICIENT_SPACE == appendResult) {
                log.warn("ConsumeQueue INSUFFICIENT_SPACE");
                consumeQueueFiles.createNewFile();
                mappedFile = consumeQueueFiles.getLastFile();
                mappedFile.appendLong(commitlogOffset);
                mappedFile.flush();
            }

            return PutMessageResult.OK;
        } catch (IOException e) {
            return PutMessageResult.FAILURE;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取消息在 commitlog 文件中总偏移值 offset
     *
     * @param topic 消息主题
     * @param seq   消费序号
     * @return
     * @throws Exception
     */
    public long getMessageOffset(String topic, String gruop) throws Exception {
        ensureFileExist(topic);

        int offset = consumeOffset.getOffset(topic, gruop);

        ConsumeQueueFiles consumeQueueFiles = mappedFileMap.get(topic);
        MappedFile mappedFile = consumeQueueFiles.getFileByOffset(offset * 8L);

        return mappedFile.getLong((offset * 8L) - mappedFile.getFromOffset());
    }

    public void incOffset(String topic, String group) throws IOException {
        this.consumeOffset.incOffset(topic, group);
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

        this.lock.lock();
        try {
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
        } finally {
            this.lock.unlock();
        }
    }

}
