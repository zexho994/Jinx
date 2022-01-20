package store.consumequeue;

import config.BrokerConfig;
import config.StoreConfig;
import lombok.extern.log4j.Log4j2;
import message.TopicUnit;
import store.MappedFileQueue;
import store.constant.FileType;
import store.constant.MessageAppendResult;
import store.constant.PutMessageResult;
import store.mappedfile.MappedFile;
import utils.ByteUtil;
import utils.This;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static common.Transaction.TRANS_HALF_OP_TOPIC;
import static common.Transaction.TRANS_HALF_TOPIC;

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

    private final Map<String/*topic*/, Map<Integer/*queueId*/, MappedFileQueue>> mappedFileMap = new ConcurrentHashMap<>();
    private final ConsumeOffset consumeOffset = ConsumeOffset.getInstance();

    /**
     * 添加一个新文件
     */
    public void createNewFile(String topic, int queueId) throws IOException {
        Map<Integer, MappedFileQueue> integerMappedFileQueueMap = this.mappedFileMap.get(topic);
        MappedFileQueue mappedFileQueue = integerMappedFileQueueMap.get(queueId);

        MappedFile lastFile = mappedFileQueue.getLastMappedFile();
        int fromOffset = lastFile.getFromOffset();
        long wrotePos = lastFile.getWrotePos();
        String nextFileOffset = String.valueOf(fromOffset + wrotePos);
        File file = new File(CONSUMER_QUEUE_FOLDER.getAbsolutePath() + File.separator + topic + File.separator + nextFileOffset);
        MappedFile mappedFile = new MappedFile(FileType.CONSUME_QUEUE, file);
        mappedFileQueue.addMappedFile(mappedFile);
    }

    public void init() throws Exception {
        try {
            this.ensureDirExist();
            if (This.isMaster()) {
                this.mkdirTopicDir();
                this.consumeOffset.init();
            }
        } catch (Exception e) {
            throw new Exception("ConsumeQueue init error. ", e);
        }
    }

    /**
     *
     */
    private void mkdirTopicDir() {
        // 用户定义的topic
        this.mkdirCustomTopicDir();
        // 事务使用的topic
        this.mkdirTranTopicDir();
    }

    private void mkdirCustomTopicDir() {
        List<TopicUnit> topics = BrokerConfig.configBody.getTopics();
        topics.forEach(topic -> {
            File topicDir = new File(CONSUMER_QUEUE_FOLDER, topic.getTopic());
            if (!topicDir.exists()) {
                topicDir.mkdir();
            }
            for (int i = 1; i <= topic.getQueue(); i++) {
                File queueDir = new File(topicDir, String.valueOf(i));
                if (!queueDir.exists()) {
                    queueDir.mkdirs();
                }
                try {
                    if (this.mappedFileMap.get(topic.getTopic()) == null) {
                        this.mappedFileMap.put(topic.getTopic(), new ConcurrentHashMap<>(topic.getQueue()));
                    }
                    if (this.mappedFileMap.get(topic.getTopic()).get(i) == null) {
                        this.mappedFileMap.get(topic.getTopic()).put(i, new MappedFileQueue());
                    }
                    if (this.mappedFileMap.get(topic.getTopic()).get(i).isEmpty()) {
                        File file = new File(queueDir, "0");
                        try {
                            file.createNewFile();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        MappedFile mappedFile = new MappedFile(FileType.CONSUME_QUEUE, file);
                        this.mappedFileMap.get(topic.getTopic()).get(i).addMappedFile(mappedFile);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 创建事务相关的文件
     */
    private void mkdirTranTopicDir() {
        initTopicQueue(TRANS_HALF_TOPIC);
        initTopicQueue(TRANS_HALF_OP_TOPIC);
    }

    private void initTopicQueue(String topicName) {
        File topicDir = new File(CONSUMER_QUEUE_FOLDER, topicName);
        if (!topicDir.exists()) {
            topicDir.mkdir();
        }
        this.mappedFileMap.put(topicName, new ConcurrentHashMap<>(1));
        this.mappedFileMap.get(topicName).put(1, new MappedFileQueue());
        File queueDir = new File(topicDir, "1");
        if (!queueDir.exists()) {
            queueDir.mkdir();
        }

        File file = new File(queueDir, "0");
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            MappedFile mappedFile = new MappedFile(FileType.CONSUME_QUEUE, file);
            this.mappedFileMap.get(topicName).get(1).addMappedFile(mappedFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * consumeQueue 文件恢复
     * step1: 遍历 consumeQueue 文件夹下topic文件夹 ,如果topic文件夹为空，退出恢复流程
     * step2: 保存 topic 信息到 {@link #mappedFileMap} 中
     * step3: 遍历 topic 文件夹下queueId文件夹
     * step4: 每一个文件封装成 {@link MappedFile} 保存到 {@link #mappedFileMap} 下
     */
    public void recover() throws Exception {
        this.ensureDirExist();
        for (File file : CONSUMER_QUEUE_FOLDER.listFiles()) {
            if (file.getName().contains(".")) {
                continue;
            }
            this.mappedFileMap.put(file.getName(), new ConcurrentHashMap<>(4));
            this.recoverQueueFile(file);
        }
    }

    private void recoverQueueFile(File topicDir) throws IOException {
        if (topicDir.listFiles() == null) {
            return;
        }
        for (File queueDirs : topicDir.listFiles()) {
            if (queueDirs.getName().contains(".")) {
                continue;
            }
            MappedFileQueue mappedFileQueue = new MappedFileQueue();
            this.mappedFileMap.get(topicDir.getName()).put(Integer.valueOf(queueDirs.getName()), mappedFileQueue);
            for (File file : queueDirs.listFiles()) {
                if (file.getName().contains(".")) {
                    continue;
                }
                mappedFileQueue.addMappedFile(new MappedFile(FileType.CONSUME_QUEUE, file));
            }
            if (!mappedFileQueue.isEmpty()) {
                this.checkConsumeQueueFile(mappedFileQueue.getLastMappedFile());
            }
        }
    }

    public void checkConsumeQueueFile(MappedFile mappedFile) {
        if (mappedFile == null) {
            return;
        }

        long offset = 0;
        while (true) {
            try {
                Long l = mappedFile.getLong(offset);
                if (l == null) {
                    break;
                }
            } catch (IOException e) {
                break;
            }
            offset += MappedFile.LONG_LENGTH;
        }

        log.info("Recover ConsumeQueue success. file = {}, wrote position = {} ", mappedFile.getAbsolutePath(), offset);
        try {
            mappedFile.setWrotePos(offset);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void ensureDirExist() {
        if (CONSUMER_QUEUE_FOLDER == null) {
            CONSUMER_QUEUE_FOLDER = new File(StoreConfig.consumeQueuePath);
            if (!CONSUMER_QUEUE_FOLDER.exists()) {
                CONSUMER_QUEUE_FOLDER.mkdirs();
            }
        }
    }

    /**
     * 存储消息到 ConsumeQueue 文件中
     *
     * @param topic           消息主题
     * @param commitlogOffset 消息在commit中的偏移量,总偏移量
     */
    public PutMessageResult putMessage(String topic, int queueId, long commitlogOffset) {
        lock.lock();
        try {
            log.trace("consumeQueue put message");
            ensureFileExist(topic);

            MappedFileQueue mappedFileQueue = this.getMappedQueue(topic, queueId);
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile();
            byte[] data = ByteUtil.to(commitlogOffset);

            MessageAppendResult appendResult = mappedFile.append(data);
            if (MessageAppendResult.OK == appendResult) {
                mappedFile.flush();
            } else if (MessageAppendResult.INSUFFICIENT_SPACE == appendResult) {
                log.warn("ConsumeQueue INSUFFICIENT_SPACE");
                this.createNewFile(topic, queueId);
                mappedFile = mappedFileQueue.getLastMappedFile();
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

    /**
     * 获取消息在 commitlog 文件中总偏移值 offset
     *
     * @param topic 消息主题
     * @param group 消费组
     * @return 获取结果对象
     * @throws Exception
     */
    public Long getCommitlogOffset(String topic, int queueId, String group) throws Exception {
        ensureFileExist(topic);
        long offset = consumeOffset.getOffset(topic, String.valueOf(queueId), group);

        long off = offset * MappedFile.LONG_LENGTH;
        MappedFile mappedFile = this.getMappedQueue(topic, queueId).getFileByOffset(off);

        return mappedFile.getLong(off - mappedFile.getFromOffset());
    }

    private MappedFileQueue getMappedQueue(String topic, int queueId) {
        return this.mappedFileMap.get(topic).get(queueId);
    }

    public void incOffset(String topic, int queueId, String group) {
        try {
            this.consumeOffset.incOffset(topic, String.valueOf(queueId), group);
        } catch (IOException e) {
            log.error("consumeOffset +1 error.", e);
        }
    }

    /**
     * 确保 ConsumeQueue 文件存在
     *
     * @param topic 主题名称
     * @throws IOException
     */
    private void ensureFileExist(String topic) throws IOException {
        this.lock.lock();
        try {
            if (mappedFileMap.containsKey(topic)) {
                return;
            }

            List<TopicUnit> topics = BrokerConfig.configBody.getTopics();
            for (TopicUnit unit : topics) {
                if (!unit.getTopic().equals(topic)) {
                    continue;
                }
                File topicFolder = new File(StoreConfig.consumeQueuePath + topic);
                topicFolder.mkdirs();
                Map<Integer, MappedFileQueue> integerMappedFileQueueMap = new HashMap<>();
                for (int i = 1; i < unit.getQueue(); i++) {
                    File queueFolder = new File(topicFolder, String.valueOf(i));
                    queueFolder.mkdirs();
                    File file = new File(queueFolder.getAbsolutePath() + File.separator + "0");
                    file.createNewFile();

                    // 记录保存到 map 中
                    MappedFile mappedFile = new MappedFile(FileType.CONSUME_QUEUE, file);
                    MappedFileQueue mappedFileQueue = new MappedFileQueue();
                    mappedFileQueue.addMappedFile(mappedFile);
                    integerMappedFileQueueMap.put(i, mappedFileQueue);
                }
                this.mappedFileMap.put(topic, integerMappedFileQueueMap);
            }

        } finally {
            this.lock.unlock();
        }
    }

}
