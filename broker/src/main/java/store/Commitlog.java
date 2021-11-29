package store;

import Message.Message;
import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Zexho
 * @date 2021/11/22 10:34 上午
 */
@Log4j2
public enum Commitlog {

    /**
     * CommitLog对象
     */
    Instance;

    /**
     * commit文件夹路径 $HOME/jinx/commit
     */
    private final String COMMIT_DIR_PATH = System.getProperty("user.home") + File.separator + "jinx" + File.separator + "commitlog";
    /**
     * 文件夹对象
     */
    public static File FOLDER_COMMIT;
    private final Queue<MappedFile> mappedFileQueue = new LinkedBlockingQueue<>();

    /**
     * 初始化时候创建文件夹
     */
    public void init() throws IOException {
        FOLDER_COMMIT = new File(COMMIT_DIR_PATH);
        if (!FOLDER_COMMIT.exists()) {
            FOLDER_COMMIT.mkdirs();
        }

        // 创建第一个文件
        MappedFile mappedFile = new MappedFile("0", MappedFile.DEFAULT_FILE_SIZE);
        mappedFileQueue.offer(mappedFile);

    }


    /**
     * 存储消息
     *
     * @param message 要存储的消息对象
     * @return 存储结果
     */
    public boolean storeMessage(Message message) {
        MappedFile mappedFile = this.getMappedFile();
        try {
            mappedFile.flush();
        } catch (IOException e) {
            log.error("Failed to store message " + message, e);
            return false;
        }
        return true;
    }

    /**
     * 获取当前可用的文件
     *
     * @return
     */
    public MappedFile getMappedFile() {
        return this.mappedFileQueue.peek();
    }
}
