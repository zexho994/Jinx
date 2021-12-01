package store;

import Message.Message;
import common.MemoryCapacity;
import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

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
    public static final String COMMIT_DIR_PATH = System.getProperty("user.home") + File.separator + "jinx" + File.separator + "commitlog" + File.separator;
    /**
     * 文件夹对象
     */
    public static File COMMITLOG_FOLDER;
    /**
     * 存储 mappedFile 队列
     */
    private final Stack<MappedFile> mappedFileStack = new Stack<>();
    /**
     * 所有文件总字节偏移量
     */
    private AtomicInteger fileFormOffset = new AtomicInteger(0);
    private final Lock lock = new ReentrantLock();
    private final int DEFAULT_MAPPED_FILE_SIZE = MemoryCapacity.KB;

    /**
     * 初始化时候创建文件夹
     */
    public boolean init() {
        COMMITLOG_FOLDER = new File(COMMIT_DIR_PATH);
        int count = 0;
        if (COMMITLOG_FOLDER.exists()) {
            // 文件夹存在
            File[] files = COMMITLOG_FOLDER.listFiles();
            if (files != null) {
                List<File> fileList = Arrays.stream(files).filter(file -> !file.getName().contains(".")).collect(Collectors.toList());
                count = fileList.size();
                fileList.stream().sorted((o1, o2) -> {
                    int offset1 = Integer.parseInt(o1.getName());
                    int offset2 = Integer.parseInt(o2.getName());
                    return offset1 - offset2;
                }).forEach(file -> {
                    try {
                        int fileOffset = Integer.parseInt(file.getName());
                        int size = fileOffset - this.fileFormOffset.get();
                        this.mappedFileStack.push(new MappedFile(file, DEFAULT_MAPPED_FILE_SIZE, this));
                        this.fileFormOffset.set(fileOffset);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                if (!this.mappedFileStack.isEmpty()) {
                    String lastFileName = this.mappedFileStack.peek().getFileName();
                    this.fileFormOffset = new AtomicInteger(Integer.parseInt(lastFileName));
                }
            }
        } else {
            // 文件夹不存在
            if (!COMMITLOG_FOLDER.mkdirs()) {
                log.error("Failed to mkdir commitlog");
                return false;
            }
        }

        if (count == 0) {
            try {
                this.createFirstMappedFile(DEFAULT_MAPPED_FILE_SIZE);
            } catch (IOException e) {
                log.error("Failed create new MappedFile", e);
                return false;
            }
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

    /**
     * 在Commitlog文件夹下创建一个新文件
     *
     * @throws IOException
     */
    public void createNewMappedFile() throws IOException {
        MappedFile mappedFile = new MappedFile(String.valueOf(this.fileFormOffset.incrementAndGet()), DEFAULT_MAPPED_FILE_SIZE, this);
        this.mappedFileStack.push(mappedFile);
    }

    /**
     * 创建第一个新文件
     *
     * @param fileSize 文件大小
     * @throws IOException
     */
    public void createFirstMappedFile(int fileSize) throws IOException {
        if (COMMITLOG_FOLDER == null) {
            log.error("commitlog folder is null");
            return;
        }
        File[] files = COMMITLOG_FOLDER.listFiles();
        if (files != null) {
            if (Arrays.stream(files).anyMatch(f -> !f.getName().contains("."))) {
                log.error("There are already files in commitlog");
                return;
            }
        }

        this.mappedFileStack.push(new MappedFile("0", fileSize, this));
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
