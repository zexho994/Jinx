package store;

import Message.Message;
import common.MemoryCapacity;
import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.io.IOException;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    /**
     * 存储 mappedFile 队列
     */
    private final Stack<MappedFile> mappedFileStack = new Stack<>();
    /**
     * 所有文件总字节偏移量
     */
    private final AtomicInteger fileFormOffset = new AtomicInteger(0);
    private final Lock lock = new ReentrantLock();


    /**
     * 初始化时候创建文件夹
     */
    public boolean init() {
        FOLDER_COMMIT = new File(COMMIT_DIR_PATH);
        if (!FOLDER_COMMIT.exists()) {
            if (!FOLDER_COMMIT.mkdirs()) {
                log.error("Failed to mkdir commitlog");
                return false;
            }
        }

        try {
            MappedFile mappedFile = new MappedFile("0", 2 * MemoryCapacity.KB, this);
            mappedFileStack.push(mappedFile);
        } catch (IOException e) {
            log.error("Failed create new MappedFile", e);
            return false;
        }

        return true;
    }

    /**
     * 存储消息
     *
     * @param message 要存储的消息对象
     */
    public void storeMessage(Message message, FlushModel model) throws IOException {
        lock.lock();
        try {
            MappedFile mappedFile = this.getLastMappedFile();
            byte[] data = message.toString().getBytes();
            // 同步刷盘消息
            if (model == FlushModel.SYNC) {
                if (mappedFile.checkFileRemainSize(data.length)) {
                    // 如果剩余空间足够
                    mappedFile.appendThenFlush(data);
                } else {
                    // 旧数据存储到旧文件
                    mappedFile.flush();
                    // 新数据存储到新文件
                    this.createNewMappedFile();
                    this.getLastMappedFile().appendThenFlush(data);
                }
            } else {
                // 异步刷盘
            }
        } catch (IOException e) {
            log.error("Failed to store message " + message, e);
            throw e;
        } finally {
            lock.unlock();
        }
    }

    public void createNewMappedFile() throws IOException {
        MappedFile mappedFile = new MappedFile(String.valueOf(this.fileFormOffset.incrementAndGet()), 2 * MemoryCapacity.KB, this);
        this.mappedFileStack.push(mappedFile);
    }

    /**
     * 获取当前可用的文件
     *
     * @return 当前最新可用的文件
     */
    public MappedFile getLastMappedFile() {
        return this.mappedFileStack.peek();
    }

    public AtomicInteger getFileFormOffset() {
        return fileFormOffset;
    }
}
