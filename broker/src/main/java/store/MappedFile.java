package store;

import Message.Message;
import common.MemoryCapacity;
import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
    private String fileName;
    private int fileSize;
    private File file;
    private long fileFormOffset;
    private FileChannel fileChannel;
    private ByteBuffer byteBuffer;
    private final Lock lock = new ReentrantLock();

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileSize = fileSize;
        this.fileName = fileName;
        String filePath = Commitlog.FOLDER_COMMIT.getAbsolutePath() + File.separator + fileName + ".log";
        this.file = new File(filePath);
        this.fileFormOffset = Long.parseLong(fileName);

        ensureDirOk(file.getParent());

        boolean initSuccess = false;
        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.byteBuffer = ByteBuffer.allocate(1024 * 1024);
            initSuccess = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
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
     * @param message 要追加的消息
     */
    public void append(final Message message) throws IOException {
        lock.lock();
        try {
            final byte[] data = message.toString().getBytes();
            this.byteBuffer.put(data);
            this.byteBuffer.flip();
            this.fileChannel.write(this.byteBuffer);
            this.byteBuffer.clear();
        } finally {
            lock.unlock();
        }
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

    public void flush() throws IOException {
        lock.lock();
        try {
            this.fileChannel.force(false);
        } finally {
            lock.unlock();
        }
    }
}
