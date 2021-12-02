package store;

import Message.Message;
import lombok.extern.log4j.Log4j2;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Zexho
 * @date 2021/12/2 9:26 上午
 */
@Log4j2
public enum DefaultMessageStore implements MessageStore {

    /**
     * 对象实例
     */
    INSTANCE;

    private final Lock lock = new ReentrantLock();
    private final Commitlog commitlog = Commitlog.Instance;

    /**
     * 默认采用同步的方式
     *
     * @param message 消息对象
     */
    @Override
    public void putMessage(Message message) {
        this.putMessage(message, FlushModel.SYNC);
    }

    @Override
    public void putMessage(Message message, FlushModel flushModel) {
        lock.lock();
        try {
            MappedFile mappedFile = commitlog.getLastMappedFile();
            byte[] data = message.toString().getBytes();
            if (flushModel == FlushModel.SYNC) {
                if (mappedFile.checkFileRemainSize(data.length)) {
                    // 如果剩余空间足够
                    mappedFile.appendThenFlush(data);
                } else {
                    // 旧数据存储到旧文件
                    mappedFile.flush();
                    // 新数据存储到新文件
                    commitlog.createNewMappedFile();
                    commitlog.getLastMappedFile().appendThenFlush(data);
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

}
