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
    public static final String COMMIT_DIR_PATH = System.getProperty("user.home") + File.separator + "jinx" + File.separator + "commitlog";
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
    private AtomicInteger fileFormOffset = new AtomicInteger(0);
    private final Lock lock = new ReentrantLock();


    /**
     * 初始化时候创建文件夹
     */
    public boolean init() {
        FOLDER_COMMIT = new File(COMMIT_DIR_PATH);
        int count = 0;
        if (FOLDER_COMMIT.exists()) {
            // 文件夹存在
            File[] files = FOLDER_COMMIT.listFiles();
            if (files != null) {
                List<File> fileList = Arrays.stream(files).filter(file -> file.getName().contains(".log")).collect(Collectors.toList());
                count = fileList.size();
                fileList.stream().sorted((o1, o2) -> {
                            int end1 = o1.getName().lastIndexOf('.');
                            int end2 = o2.getName().lastIndexOf('.');
                            int offset1 = Integer.parseInt(o1.getName().substring(0, end1));
                            int offset2 = Integer.parseInt(o2.getName().substring(0, end2));
                            return offset1 - offset2;
                        })
                        .forEach(file -> {
                            try {
                                this.mappedFileStack.push(new MappedFile(file, 2 * MemoryCapacity.KB, this));
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
                String lastFileName = this.mappedFileStack.peek().getFileName();
                int logIdx = lastFileName.lastIndexOf('.');
                this.fileFormOffset = new AtomicInteger(Integer.parseInt(lastFileName.substring(0, logIdx)));
            }
        } else {
            // 文件夹不存在
            if (!FOLDER_COMMIT.mkdirs()) {
                log.error("Failed to mkdir commitlog");
                return false;
            }
        }

        if (count == 0) {
            try {
                this.createFirstMappedFile(2 * MemoryCapacity.KB);
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

    public void createNewMappedFile() throws IOException {
        MappedFile mappedFile = new MappedFile(this.fileFormOffset.incrementAndGet() + ".log", 2 * MemoryCapacity.KB, this);
        this.mappedFileStack.push(mappedFile);
    }

    public void createFirstMappedFile(int fileSize) throws IOException {
        this.mappedFileStack.push(new MappedFile("0.log", fileSize, this));
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
