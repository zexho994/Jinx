package store;

import Message.Message;
import lombok.extern.log4j.Log4j2;

import java.io.File;

/**
 * @author Zexho
 * @date 2021/12/2 10:30 上午
 */
@Log4j2
public class ConsumeQueue {

    private ConsumeQueue() {
    }

    private static class Inner {
        private static final ConsumeQueue INSTANCE = new ConsumeQueue();
    }

    public static ConsumeQueue getInstance() {
        return Inner.INSTANCE;
    }

    public static final String CONSUMER_FOLDER_DIR_PATH = System.getProperty("user.home") + File.separator + "jinx" + File.separator + "consumeQueue" + File.separator;
    public static File CONSUMER_QUEUE_FOLDER;

    public boolean init() {
        CONSUMER_QUEUE_FOLDER = new File(CONSUMER_FOLDER_DIR_PATH);
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
    public void putMessage(Message message, int offset, int msgSize) {

    }

}
