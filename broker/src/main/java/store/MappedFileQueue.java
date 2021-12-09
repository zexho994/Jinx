package store;

import store.mappedfile.MappedFile;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Zexho
 * @date 2021/12/8 2:27 下午
 */
public class MappedFileQueue {

    private final CopyOnWriteArrayList<MappedFile> queue = new CopyOnWriteArrayList<>();

    public void addMappedFile(MappedFile mappedFile) {
        this.queue.add(mappedFile);
    }

    public MappedFile getByIndex(int idx) {
        if (idx > this.queue.size()) {
            throw new RuntimeException("Idx should be less than size,idx = " + idx + ",size = " + this.queue.size());
        }
        return queue.get(idx);
    }

    public MappedFile getLastMappedFile() {
        if (queue.isEmpty()) {
            throw new RuntimeException("mappedFile is empty");
        }
        return queue.get(queue.size() - 1);
    }

    public boolean isEmpty() {
        return this.queue.isEmpty();
    }

    public int size() {
        return this.queue.size();
    }

    /**
     * 根据偏移量找到对应存储文件
     *
     * @param offset 要查找消息的总偏移量
     * @return
     */
    public MappedFile getFileByOffset(long offset) throws Exception {
        if (offset < 0) {
            throw new Exception("offset must be positive");
        }
        for (int i = queue.size() - 1; i >= 0; i--) {
            MappedFile mappedFile = queue.get(i);
            if (mappedFile.getFromOffset() <= offset) {
                return mappedFile;
            }
        }
        throw new Exception("Get file by offset fail, offset = " + offset);
    }
}
