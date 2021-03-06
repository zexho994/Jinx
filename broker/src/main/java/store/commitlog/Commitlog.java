package store.commitlog;

import config.StoreConfig;
import lombok.extern.log4j.Log4j2;
import message.Message;
import store.MappedFileQueue;
import store.constant.FileType;
import store.constant.FlushModel;
import store.constant.MessageAppendResult;
import store.constant.PutMessageResult;
import store.mappedfile.MappedFile;
import store.model.CommitPutMessageResult;
import utils.ArrayUtils;
import utils.ByteUtil;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Zexho
 * @date 2021/11/22 10:34 上午
 */
@Log4j2
public class Commitlog {

    private Commitlog() {
    }


    private static class Inner {
        private static final Commitlog INSTANCE = new Commitlog();
    }

    public static Commitlog getInstance() {
        return Inner.INSTANCE;
    }

    /**
     * 文件夹对象
     */
    private static File COMMITLOG_FOLDER;
    /**
     * 存储 mappedFile 队列
     */
    private final MappedFileQueue mappedFileQueue = new MappedFileQueue();
    /**
     * 所有文件总字节偏移量
     */
    private final AtomicLong commitlogOffset = new AtomicLong(0);
    private final Lock lock = new ReentrantLock();

    /**
     * commitlog 初始化设置
     * step1:
     * - 如果mappedFileQueue不为空，说明commitlog进行了恢复,设置wrotePos和fileFormOffset
     * - 如果为空，创建一个"0"文件即可
     */
    public void init() throws Exception {
        ensureDirExist();
        createDefaultFile();
    }

    private void createDefaultFile() throws IOException {
        if (this.mappedFileQueue.isEmpty()) {
            File file = new File(COMMITLOG_FOLDER, "0");
            this.mappedFileQueue.addMappedFile(new MappedFile(FileType.COMMITLOG, file));
        }
    }

    /**
     * 确保文件夹存在
     *
     * @return true已经存在. false不存在
     */
    private void ensureDirExist() {
        if (COMMITLOG_FOLDER == null) {
            COMMITLOG_FOLDER = new File(StoreConfig.commitlogPath);
            COMMITLOG_FOLDER.mkdirs();
        }
    }

    /**
     * commitlog 文件恢复
     * step1: 如果 commitlog 文件不存在，不需要恢复
     * step2: 遍历 commitlog 文件夹下文件，从偏移量低的文件开始恢复(需要进行排序)
     * step3: 封装文件成 MappedFile 对象，然后保存到 {@link #mappedFileQueue} 中
     * step4: 设置wrotePos和fileFormOffset
     *
     * @throws IOException
     */
    public void recover() throws IOException {
        this.ensureDirExist();
        File[] files = COMMITLOG_FOLDER.listFiles();
        if (files == null) {
            return;
        }
        Arrays.stream(files).filter(file -> !file.getName().contains("."))
                .sorted((o1, o2) -> {
                    int offset1 = Integer.parseInt(o1.getName());
                    int offset2 = Integer.parseInt(o2.getName());
                    return offset1 - offset2;
                }).forEach(file -> {
                    try {
                        this.mappedFileQueue.addMappedFile(new MappedFile(FileType.COMMITLOG, file));
                        log.info("Recover Commitlog success. file = {}, wrote position = {}", file.getAbsolutePath(), file.getName());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

        // 如果进行了恢复，设置wrotePos和fileFormOffset
        if (!this.mappedFileQueue.isEmpty()) {
            MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile();
            long offset = 0;
            while (true) {
                Integer size = lastMappedFile.getInt(offset);
                if (size == null) {
                    break;
                }
                Message message = lastMappedFile.loadMessage(offset + MappedFile.INT_LENGTH, size);
                if (message == null) {
                    break;
                }
                offset += MappedFile.INT_LENGTH + size;
            }
            try {
                lastMappedFile.setWrotePos(offset);
                this.commitlogOffset.set(lastMappedFile.getFromOffset() + offset);
            } catch (IOException e) {
                log.error("set wrote position error. ", e);
            }
        }
    }

    /**
     * 保存消息到 Commitlog 目录下中
     *
     * @param message    要保存的对象
     * @param flushModel 消息的刷盘类型
     * @return 执行结果, 有以下几种：
     * {@link PutMessageResult#OK} 成功
     * {@link PutMessageResult#FAILURE} 某种原因导致失败
     */
    public CommitPutMessageResult putMessage(final Message message, final FlushModel flushModel) {
        lock.lock();
        try {
            log.trace("commitlog put message");
            long fileWriteOffset = this.mappedFileQueue.getLastMappedFile().getFromOffset() + this.mappedFileQueue.getLastMappedFile().getWrotePos();
            byte[] messagebyte = ByteUtil.to(message);
            byte[] size = ByteUtil.to(messagebyte.length);
            byte[] data = ArrayUtils.merge(size, messagebyte);

            MessageAppendResult appendResult = this.mappedFileQueue.getLastMappedFile().append(data);
            if (flushModel == FlushModel.SYNC) {
                // 同步刷盘,在追加后立即执行flush
                switch (appendResult) {
                    case OK:
                        this.mappedFileQueue.getLastMappedFile().flush();
                        break;
                    case INSUFFICIENT_SPACE:
                        log.trace("commitlog insufficient space");
                        this.createNewMappedFile();
                        fileWriteOffset = this.mappedFileQueue.getLastMappedFile().getFromOffset();
                        this.mappedFileQueue.getLastMappedFile().append(data);
                        this.mappedFileQueue.getLastMappedFile().flush();
                        break;
                    default:
                        log.error("AppendResult type error");
                        break;
                }
            } else {
                // 异步刷盘
            }

            this.commitlogOffset.getAndAdd(data.length);
            return CommitPutMessageResult.ok(fileWriteOffset, messagebyte.length);
        } catch (IOException e) {
            log.error("Failed to store message " + message, e);
            return CommitPutMessageResult.error();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取消息
     *
     * @param offset 在文件中的偏移量
     * @return 目标消息对象
     */
    public Message getMessage(long offset) throws IOException {
        MappedFile mappedFile = this.getFileByOffset(offset);
        Integer size = mappedFile.getInt(offset - mappedFile.getFromOffset());
        int pos = (int) (offset - mappedFile.getFromOffset() + MappedFile.INT_LENGTH);
        try {
            return mappedFile.loadMessage(pos, size);
        } catch (IOException e) {
            throw new IOException("load message error, offset = " + offset, e);
        }
    }

    private int getMessageSize(long offset) throws IOException {
        MappedFile mappedFile = this.getFileByOffset(offset);
        return mappedFile.getInt(offset - mappedFile.getFromOffset());
    }

    /**
     * 获取offset之后的所有数据
     *
     * @param offset commitlog偏移量
     */
    public List<Message> getMessageByOffset(long offset) throws IOException {
        List<Message> messages = new LinkedList<>();
        while (offset < this.getCommitlogOffset()) {
            int messageSize = this.getMessageSize(offset);
            Message message = this.getMessage(offset);
            messages.add(message);
            offset += messageSize + MappedFile.INT_LENGTH;
        }
        return messages;
    }

    private MappedFile getFileByOffset(long offset) {
        for (int i = this.mappedFileQueue.size() - 1; i >= 0; i--) {
            MappedFile mappedFile = this.mappedFileQueue.getByIndex(i);
            if (mappedFile.getFromOffset() <= offset) {
                return mappedFile;
            }
        }

        log.error("get file by offset fail, offset = " + offset);
        return null;
    }

    /**
     * 在Commitlog文件夹下创建一个新文件
     *
     * @throws IOException 创建新文件时发送异常
     */
    public void createNewMappedFile() throws IOException {
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile();
        int fileFormOffset = Integer.parseInt(lastMappedFile.getFileName());
        long wrotePos = lastMappedFile.getWrotePos();
        String fileName = String.valueOf(fileFormOffset + wrotePos + 1);
        MappedFile mappedFile = new MappedFile(FileType.COMMITLOG, fileName);
        this.mappedFileQueue.addMappedFile(mappedFile);
    }

    public long getCommitlogOffset() {
        return commitlogOffset.get();
    }

    public boolean haveCommitlog() {
        return !this.mappedFileQueue.isEmpty();
    }

}
