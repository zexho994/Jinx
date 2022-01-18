package store.consumequeue;

import config.StoreConfig;
import lombok.extern.log4j.Log4j2;
import store.constant.FileType;
import store.mappedfile.MappedFile;
import utils.ByteUtil;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Zexho
 * @date 2021/12/6 9:47 上午
 */
@Log4j2
public class ConsumeOffset {

    public static File CONSUME_OFFSET_FOLDER;

    private ConsumeOffset() {
    }

    private static class Inner {
        private static final ConsumeOffset INSTANCE = new ConsumeOffset();
    }

    public static ConsumeOffset getInstance() {
        return ConsumeOffset.Inner.INSTANCE;
    }

    /**
     * key = topic
     * val = Map<queueId,MappedFile>>
     */
    private final Map<String, Map<String, Map<String, MappedFile>>> consumeOffsetMap = new ConcurrentHashMap<>();

    void init() {
        try {
            this.recover();
        } catch (Exception e) {
            log.error("ConsumeOffset init error", e);
        }
    }

    private void ensureDirExist() {
        if (CONSUME_OFFSET_FOLDER == null) {
            CONSUME_OFFSET_FOLDER = new File(StoreConfig.consumeOffsetPath);
            if (!CONSUME_OFFSET_FOLDER.exists()) {
                CONSUME_OFFSET_FOLDER.mkdirs();
            }
        }
    }

    public void recover() throws Exception {
        this.ensureDirExist();
        String topic, queue;
        for (File topicDir : CONSUME_OFFSET_FOLDER.listFiles()) {
            if ((topic = topicDir.getName()).contains(".")) {
                continue;
            }
            this.addTopic(topic);
            for (File queueDir : topicDir.listFiles()) {
                if ((queue = queueDir.getName()).contains(".")) {
                    continue;
                }
                this.addQueue(topic, queue);
                for (File file : queueDir.listFiles()) {
                    MappedFile mf = new MappedFile(FileType.CONSUME_OFFSET, file);
                    this.addConsumeGroup(topic, queue, file.getName(), new MappedFile(FileType.CONSUME_OFFSET, file));
                    log.info("Recover ConsumeOffset success. file = {} , consume offset = {}", mf.getAbsolutePath(), mf.getInt(0));
                }
            }
        }
    }

    /**
     * 添加一个topic，同时创建一个topic的文件夹
     *
     * @param topic
     */
    private void addTopic(String topic) {
        if (this.consumeOffsetMap.containsKey(topic)) {
            return;
        }
        File file = new File(StoreConfig.consumeOffsetPath + topic);
        if (!file.exists()) {
            file.mkdir();
        }
        this.consumeOffsetMap.put(topic, new ConcurrentHashMap<>(4));
    }

    private void addQueue(String topic, String queueId) {
        this.addTopic(topic);
        if (this.consumeOffsetMap.get(topic).containsKey(queueId)) {
            return;
        }
        File file = new File(StoreConfig.consumeOffsetPath + topic + File.separator + queueId);
        if (!file.exists()) {
            file.mkdir();
        }
        this.consumeOffsetMap.get(topic).put(queueId, new ConcurrentHashMap<>());
    }

    private void addConsumeGroup(String topic, String queueId, String group, MappedFile mappedFile) {
        this.addQueue(topic, queueId);
        if (this.consumeOffsetMap.get(topic).get(queueId).containsKey(group)) {
            return;
        }
        File file = new File(StoreConfig.consumeOffsetPath + topic + File.separator + queueId + File.separator + group);
        if (!file.exists()) {
            try {
                file.createNewFile();
                if (mappedFile == null) {
                    mappedFile = new MappedFile(FileType.CONSUME_OFFSET, file);
                    mappedFile.append(ByteUtil.to(0));
                }
            } catch (IOException e) {
                log.warn("failed to create consume offset file error. ", e);
            }
        }
        this.consumeOffsetMap.get(topic).get(queueId).put(group, mappedFile);
    }

    /**
     * 获取group的在queueId下的消费进度
     *
     * @param topic        主题
     * @param queueId      队列id
     * @param consumeGroup 消费组
     * @return
     */
    public long getOffset(String topic, String queueId, String consumeGroup) throws Exception {
        this.addConsumeGroup(topic, String.valueOf(queueId), consumeGroup, null);
        return this.getFile(topic, queueId, consumeGroup).getInt(0);
    }

    /**
     * 消费记录+1
     *
     * @param topic        消息主题
     * @param consumeGroup 消费组
     */
    public void incOffset(String topic, String queue, String consumeGroup) throws IOException {
        MappedFile file = this.getFile(topic, queue, consumeGroup);
        try {
            int seq = file.getInt(0);
            log.debug("inc offset ,topic = {}, queue = {}, group = {}, seq = {}", topic, queue, consumeGroup, seq);
            file.updateInt(0, seq + 1);
        } catch (IOException e) {
            throw new IOException("accumulation offset error, topic = " + topic + ", consumeGroup = " + consumeGroup, e);
        }
    }

    public MappedFile getFile(String topic, String queue, String consumeGroup) {
        return this.consumeOffsetMap.get(topic).get(queue).get(consumeGroup);
    }

}
