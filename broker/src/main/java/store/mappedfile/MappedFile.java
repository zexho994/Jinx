package store.mappedfile;

import common.MemoryCapacity;
import lombok.extern.log4j.Log4j2;
import message.Message;
import store.constant.FileType;
import store.constant.MessageAppendResult;
import utils.ByteUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 磁盘文件的映射
 *
 * @author Zexho
 * @date 2021/11/29 2:23 下午
 */
@Log4j2
public class MappedFile {

    public static final int INT_LENGTH = 4;
    public static final int LONG_LENGTH = 8;

    private final File file;
    private RandomAccessFile randomAccessFile;
    private FileChannel accessChannel;
    private ByteBuffer byteBuffer;
    /**
     * 文件名
     */
    private final String fileName;
    private final int fromOffset;
    /**
     * 文件大小
     */
    private final int fileSize;
    /**
     * 文件写入偏移量
     */
    private final AtomicLong wrotePos = new AtomicLong(0);
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
        if (fileType == FileType.COMMITLOG || fileType == FileType.CONSUME_QUEUE) {
            this.fromOffset = Integer.parseInt(fileName);
        } else {
            this.fromOffset = 0;
        }
        this.fileSize = fileType.fileSize;
        init();
    }

    private void init() throws IOException {
        ensureDirExist(this.file.getParent());

        boolean initSuccess = false;
        try {
            this.randomAccessFile = new RandomAccessFile(file, "rw");
            this.accessChannel = randomAccessFile.getChannel();
            this.byteBuffer = ByteBuffer.allocate(MemoryCapacity.MB);
            initSuccess = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + file.getName(), e);
            throw e;
        } finally {
            if (!initSuccess && this.accessChannel != null) {
                this.accessChannel.close();
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
        if (!this.checkRemainSize(data.length)) {
            return MessageAppendResult.INSUFFICIENT_SPACE;
        }
        log.info("append data to mappedFile,\n====> file = {},\n====> wrotePos = {}, size = {}\n", this.file.getAbsolutePath(), this.wrotePos, data.length);

        this.byteBuffer.put(data);
        this.byteBuffer.flip();
        this.accessChannel.write(this.byteBuffer);
        this.byteBuffer.clear();

        this.wrotePos.getAndAdd(data.length);

        return MessageAppendResult.OK;
    }

    public MessageAppendResult appendInt(final int n) throws IOException {
        if (!checkRemainSize(INT_LENGTH)) {
            return MessageAppendResult.INSUFFICIENT_SPACE;
        }
        this.randomAccessFile.writeInt(n);
        this.wrotePos.getAndAdd(INT_LENGTH);
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
        this.accessChannel.force(false);
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

    public Long getLong(long offset) throws IOException {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            if (offset + LONG_LENGTH > randomAccessFile.length()) {
                return null;
            }
            randomAccessFile.seek(offset);
            return randomAccessFile.readLong();
        }
    }

    public Integer getInt(long offset) throws IOException {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            if (offset + INT_LENGTH > randomAccessFile.length()) {
                return null;
            }
            randomAccessFile.seek(offset);
            return randomAccessFile.readInt();
        }
    }

    /**
     * 更新int值
     *
     * @param offset 偏移量
     * @param n      新值
     * @throws IOException
     */
    public void updateInt(int offset, int n) throws IOException {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            randomAccessFile.seek(offset);
            randomAccessFile.writeInt(n);
        }
    }

    public Message loadMessage(long offset, int size) throws IOException {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            if (offset + size > randomAccessFile.length()) {
                return null;
            }
            randomAccessFile.seek(offset);
            byte[] b = new byte[size];
            randomAccessFile.read(b);
            return ByteUtil.to(b, Message.class);
        }
    }

    public String getFileName() {
        return this.fileName;
    }

    public int getFromOffset() {
        return this.fromOffset;
    }

    public boolean checkRemainSize(long size) {
        return this.wrotePos.get() + size <= this.fileSize;
    }

    public long getWrotePos() {
        return wrotePos.get();
    }

    public void setWrotePos(long pos) throws IOException {
        this.wrotePos.set(pos);
        this.accessChannel.position(pos);
    }

    public String getAbsolutePath() {
        return this.file.getAbsolutePath();
    }

}
