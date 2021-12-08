package store;

import store.mappedfile.MappedFile;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Zexho
 * @date 2021/12/8 2:27 下午
 */
public class MappedFileQueue {

    CopyOnWriteArrayList<MappedFile> queue = new CopyOnWriteArrayList();

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
}
