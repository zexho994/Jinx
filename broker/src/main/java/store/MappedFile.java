package store;

import common.MemoryCapacity;
import lombok.extern.log4j.Log4j2;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 磁盘文件的映射
 *
 * @author Zexho
 * @date 2021/11/29 2:23 下午
 */
@Log4j2
public class MappedFile {

    private final File file;
    private FileChannel fileChannel;
    private ByteBuffer byteBuffer;
    /**
     * 文件名
     */
    private final String fileName;
    /**
     * 文件大小
     */
    private final int fileSize;
    /**
     * 文件写入偏移量
     */
    private final AtomicInteger wrotePos;
    /**
     * 文件类型
     */
    private final FileType fileType;

    public MappedFile(final FileType fileType, final String fileName) throws IOException {
        this(fileType, new File(fileType.basePath + fileName));
    }

    public MappedFile(final FileType fileType, final File file) throws IOException {
        this.fileType = fileType;
        this.file = file;
        this.fileName = file.getName();
        this.wrotePos = new AtomicInteger(0);
        this.fileSize = fileType.fileSize;
        init();
    }

    private void init() throws IOException {
        ensureDirExist(this.file.getParent());

        boolean initSuccess = false;
        try {
            this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
            this.byteBuffer = ByteBuffer.allocate(MemoryCapacity.MB);
            initSuccess = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + file.getName(), e);
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
     * @return 返回结果有几种：
     * {@link MessageAppendResult#INSUFFICIENT_SPACE} 空间不够用
     * {@link MessageAppendResult#OK} 追加成功
     */
    public MessageAppendResult append(final byte[] data) throws IOException {
        if (!this.checkRemainSize(data)) {
            return MessageAppendResult.INSUFFICIENT_SPACE;
        }

        this.byteBuffer.put(data);
        this.byteBuffer.flip();
        this.fileChannel.write(this.byteBuffer);
        this.byteBuffer.clear();

        this.wrotePos.getAndAdd(data.length);

        return MessageAppendResult.OK;
    }

    private void ensureDirExist(final String dirName) {
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

    public Queue<String> load() throws IOException {
        InputStream inputStream = new FileInputStream(this.file);
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

        Queue<String> fileData = new LinkedList<>();
        for (String data = ""; data != null; ) {
            data = bufferedReader.readLine();
            fileData.add(data);
        }
        return fileData;
    }

    public String getFileName() {
        return this.fileName;
    }

    public boolean checkRemainSize(byte[] data) {
        int curSize = this.wrotePos.get() + data.length;
        return curSize <= this.fileSize;
    }

    public int getWrotePos() {
        return wrotePos.get();
    }
}
