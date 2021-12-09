package store.consumequeue;

import lombok.extern.log4j.Log4j2;
import store.MappedFileQueue;
import store.constant.FileType;
import store.constant.MessageAppendResult;
import store.constant.PutMessageResult;
import store.mappedfile.MappedFile;
import utils.ArrayUtils;
import utils.ByteUtil;

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

    /**
     * Key: topic
     * Val: fileQueue
     */
    private final Map<String, MappedFileQueue> mappedFileMap = new ConcurrentHashMap<>();
    private final ConsumeOffset consumeOffset = ConsumeOffset.getInstance();

    /**
     * 添加一个新文件
     */
    public void createNewFile(String topic) throws IOException {
        MappedFileQueue mappedFileQueue = this.mappedFileMap.get(topic);
        MappedFile lastFile = mappedFileQueue.getLastMappedFile();
        int fromOffset = lastFile.getFromOffset();
        long wrotePos = lastFile.getWrotePos();
        String nextFileOffset = String.valueOf(fromOffset + wrotePos);
        File file = new File(CONSUMER_QUEUE_FOLDER.getAbsolutePath() + File.separator + topic + File.separator + nextFileOffset);
        MappedFile mappedFile = new MappedFile(FileType.CONSUME_QUEUE, file);
        mappedFileQueue.addMappedFile(mappedFile);
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
    public PutMessageResult putMessage(String topic, long commitlogOffset, int size) {
        lock.lock();
        try {
            ensureFileExist(topic);

            MappedFileQueue mappedFileQueue = mappedFileMap.get(topic);
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile();

            byte[] a1 = ByteUtil.to(commitlogOffset);
            byte[] a2 = ByteUtil.to(size);
            byte[] merge = ArrayUtils.merge(a1, a2);

            MessageAppendResult appendResult = mappedFile.append(merge);
            if (MessageAppendResult.OK == appendResult) {
                mappedFile.flush();
            } else if (MessageAppendResult.INSUFFICIENT_SPACE == appendResult) {
                log.warn("ConsumeQueue INSUFFICIENT_SPACE");
                this.createNewFile(topic);
                mappedFile = mappedFileQueue.getLastMappedFile();
                mappedFile.append(merge);
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
     * @param gruop 消费组
     * @return 获取结果对象
     * @throws Exception
     */
    public GetCommitlogOffset getCommitlogOffset(String topic, String gruop) throws Exception {
        ensureFileExist(topic);

        int offset = consumeOffset.getOffset(topic, gruop);

        MappedFileQueue consumeQueueFiles = mappedFileMap.get(topic);
        MappedFile mappedFile = consumeQueueFiles.getFileByOffset(offset * 12L);

        long commitOffset = mappedFile.getLong((offset * 12L) - mappedFile.getFromOffset());
        int msgSize = mappedFile.getInt((int) ((offset * 12L) - mappedFile.getFromOffset() + 8));
        return new GetCommitlogOffset(commitOffset, msgSize);
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
            MappedFileQueue mappedFileQueue = new MappedFileQueue();
            mappedFileQueue.addMappedFile(mappedFile);
            this.mappedFileMap.put(topic, mappedFileQueue);
        } finally {
            this.lock.unlock();
        }
    }

}
