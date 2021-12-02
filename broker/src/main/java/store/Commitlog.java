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
    private final AtomicInteger fileFormOffset = new AtomicInteger(0);
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
                        this.fileFormOffset.set(fileOffset);
                        this.mappedFileStack.push(new MappedFile(file, DEFAULT_MAPPED_FILE_SIZE));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                if (!this.mappedFileStack.isEmpty()) {
                    String lastFileName = this.mappedFileStack.peek().getFileName();
                    this.fileFormOffset.set(Integer.parseInt(lastFileName));
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

    public void putMessage(final Message message, final FlushModel flushModel) {
        byte[] data = message.toString().getBytes();

        lock.lock();
        try {
            MessageAppendResult appendResult = this.getLastMappedFile().append(data);
            if (flushModel == FlushModel.SYNC) {
                // 同步刷盘,在追加后立即执行flush
                switch (appendResult) {
                    case OK:
                        this.getLastMappedFile().flush();
                        break;
                    case INSUFFICIENT_SPACE:
                        this.createNewMappedFile();
                        this.getLastMappedFile().append(data);
                        this.getLastMappedFile().flush();
                        break;
                    default:
                        log.error("AppendResult type error");
                        break;
                }
            } else {
                // 异步刷盘

            }
        } catch (IOException e) {
            log.error("Failed to store message " + message, e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 在Commitlog文件夹下创建一个新文件
     *
     * @throws IOException 创建新文件时发送异常
     */
    public void createNewMappedFile() throws IOException {
        MappedFile lastMappedFile = this.getLastMappedFile();
        int fileFormOffset = lastMappedFile.getFileFormOffset();
        int wrotePos = lastMappedFile.getWrotePos();
        String fileName = String.valueOf(fileFormOffset + wrotePos + 1);
        MappedFile mappedFile = new MappedFile(fileName, DEFAULT_MAPPED_FILE_SIZE);
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

        this.mappedFileStack.push(new MappedFile("0", fileSize));
    }

    /**
     * 获取当前可用的文件
     *
     * @return 当前最新可用的文件
     */
    public MappedFile getLastMappedFile() {
        return this.mappedFileStack.peek();
    }

}
