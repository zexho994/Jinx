package store.consumequeue;

import lombok.extern.log4j.Log4j2;
import store.constant.FileType;
import store.mappedfile.MappedFile;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Zexho
 * @date 2021/12/6 9:47 上午
 */
@Log4j2
class ConsumeOffset {

    public static File CONSUME_OFFSET_FOLDER;

    private ConsumeOffset() {
    }

    private static class Inner {
        private static final ConsumeOffset INSTANCE = new ConsumeOffset();
    }

    public static ConsumeOffset getInstance() {
        return ConsumeOffset.Inner.INSTANCE;
    }

    private final Map<String, Map<String, MappedFile>> consumeOffsetMap = new ConcurrentHashMap<>();

    void init() {
        try {
            this.ensureDirExist();
            this.recover();
        } catch (Exception e) {
            log.error("ConsumeOffset init error", e);
        }
    }

    private void ensureDirExist() {
        if (CONSUME_OFFSET_FOLDER == null) {
            CONSUME_OFFSET_FOLDER = new File(FileType.CONSUME_OFFSET.basePath);
            CONSUME_OFFSET_FOLDER.mkdirs();
        }
    }

    public void recover() throws Exception {
        if (CONSUME_OFFSET_FOLDER == null) {
            throw new Exception("CONSUME_OFFSET_FOLDER is null");
        }
        Arrays.stream(CONSUME_OFFSET_FOLDER.listFiles())
                .filter(file -> !file.getName().contains("."))
                .forEach(topicDir -> {
                    Map<String, MappedFile> mappedFileMap = new ConcurrentHashMap<>(16);
                    this.consumeOffsetMap.put(topicDir.getName(), mappedFileMap);
                    Arrays.stream(topicDir.listFiles())
                            .filter(file -> !file.getName().contains("."))
                            .forEach(file -> {
                                try {
                                    MappedFile mf = new MappedFile(FileType.CONSUME_OFFSET, file);
                                    mappedFileMap.put(file.getName(), mf);
                                    log.info("Recover ConsumeOffset success. file = {} , consume offset = {}", mf.getAbsolutePath(), mf.getInt(0));
                                } catch (IOException e) {
                                    log.error("Create mapped file error. ", e);
                                }
                            });
                });
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

        File file = new File(CONSUME_OFFSET_FOLDER, topic);
        file.mkdirs();

        this.consumeOffsetMap.put(topic, new ConcurrentHashMap<>(4));
    }

    private void addConsumeGroup(String topic, String consumeGroup) throws IOException {
        Map<String, MappedFile> topicMap = this.consumeOffsetMap.get(topic);
        if (topicMap.containsKey(consumeGroup)) {
            return;
        }
        File file = new File(CONSUME_OFFSET_FOLDER, topic + File.separator + consumeGroup);
        try {
            file.createNewFile();
        } catch (IOException e) {
            throw new IOException("Failed to create a new file" + e);
        }

        try {
            topicMap.put(consumeGroup, new MappedFile(FileType.CONSUME_OFFSET, file));
        } catch (IOException e) {
            throw new IOException("Failed to create mappedFile." + e);
        }

        try {
            topicMap.get(consumeGroup).appendInt(0);
            topicMap.get(consumeGroup).flush();
        } catch (IOException e) {
            throw new IOException("Failed to append number(int) to file,consumeGroup = " + consumeGroup + ", n = " + 0, e);
        }
    }

    /**
     * 获取group的消费进度
     *
     * @param topic
     * @param consumeGroup
     * @return
     */
    public long getOffset(String topic, String consumeGroup) throws Exception {
        Map<String, MappedFile> topicMap = consumeOffsetMap.get(topic);
        if (topicMap == null) {
            this.addTopic(topic);
            topicMap = consumeOffsetMap.get(topic);
        }

        MappedFile mappedFile = topicMap.get(consumeGroup);
        if (mappedFile == null) {
            try {
                addConsumeGroup(topic, consumeGroup);
            } catch (IOException e) {
                throw new IOException("Failed to add consumeGroup,topic = " + topic + ", consumeGroup = " + consumeGroup, e);
            }
            mappedFile = topicMap.get(consumeGroup);
        }

        return mappedFile.getInt(0);
    }

    /**
     * 消费记录+1
     *
     * @param topic        消息主题
     * @param consumeGroup 消费组
     */
    public void incOffset(String topic, String consumeGroup) throws IOException {
        MappedFile file = this.getFile(topic, consumeGroup);
        try {
            int seq = file.getInt(0);
            file.updateInt(0, seq + 1);
        } catch (IOException e) {
            throw new IOException("accumulation offset error, topic = " + topic + ", consumeGroup = " + consumeGroup, e);
        }
    }

    public MappedFile getFile(String topic, String consumeGroup) {
        Map<String, MappedFile> topicMap = this.consumeOffsetMap.get(topic);
        return topicMap.get(consumeGroup);
    }
}
