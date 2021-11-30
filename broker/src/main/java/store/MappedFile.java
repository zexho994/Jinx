package store;

import common.MemoryCapacity;
import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 磁盘文件的映射
 *
 * @author Zexho
 * @date 2021/11/29 2:23 下午
 */
@Log4j2
public class MappedFile {

    public static final int DEFAULT_FILE_SIZE = MemoryCapacity.GB;
    private FileChannel fileChannel;
    private ByteBuffer byteBuffer;
    private final Lock lock = new ReentrantLock();
    private final Commitlog commitlog;

    /**
     * 当前文件剩余字节大小
     */
    private final AtomicInteger remainFileSize = new AtomicInteger(0);


    public MappedFile(final String fileName, Commitlog commitlog) throws IOException {
        this(fileName, DEFAULT_FILE_SIZE, commitlog);
    }

    public MappedFile(final String fileName, final int fileSize, Commitlog commitlog) throws IOException {
        init(fileName, fileSize);
        this.commitlog = commitlog;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.remainFileSize.getAndAdd(fileSize);
        String filePath = Commitlog.FOLDER_COMMIT.getAbsolutePath() + File.separator + fileName + ".log";
        File file = new File(filePath);

        ensureDirOk(file.getParent());

        boolean initSuccess = false;
        try {
            this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
            this.byteBuffer = ByteBuffer.allocate(MemoryCapacity.MB);
            initSuccess = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + fileName, e);
            throw e;
        } finally {
            if (!initSuccess && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    /**
     * 追加消息到文件末尾
     *
     * @param data 要追加的数据
     */
    public void append(final byte[] data) throws IOException {
        this.byteBuffer.put(data);
        this.byteBuffer.flip();
        this.fileChannel.write(this.byteBuffer);
        this.byteBuffer.clear();

        this.commitlog.getFileFormOffset().getAndUpdate(n -> n + data.length);
        this.remainFileSize.getAndUpdate(n -> n - data.length);
    }

    private void ensureDirOk(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    /**
     * 执行刷盘
     *
     * @throws IOException
     */
    public void flush() throws IOException {
        this.fileChannel.force(false);
    }

    /**
     * 追加消息，然后存储到文件中
     *
     * @param data 存储的消息
     * @throws IOException
     */
    public void appendThenFlush(final byte[] data) throws IOException {
        lock.lock();
        try {
            this.append(data);
            this.flush();
        } finally {
            lock.unlock();
        }
    }

    public boolean checkFileRemainSize(int size) {
        return this.remainFileSize.get() > size;
    }


}
