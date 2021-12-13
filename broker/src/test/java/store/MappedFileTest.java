package store;

import message.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import store.constant.FileType;
import store.mappedfile.MappedFile;
import utils.ByteUtil;

import java.io.IOException;
import java.util.Queue;

class MappedFileTest {


    @Test
    void load() {
        try {
            MappedFile mappedFile = new MappedFile(FileType.COMMITLOG, "0");
            Queue<String> load = mappedFile.load();
            Assertions.assertTrue(load.size() > 0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void loadMessage() throws IOException {
        MappedFile mappedFile = new MappedFile(FileType.COMMITLOG, "0");
        Message message = new Message();
        byte[] bytes = ByteUtil.to(message);
        int size = bytes.length;
        byte[] s = ByteUtil.to(size);
        mappedFile.append(s);
        mappedFile.append(bytes);
        mappedFile.flush();

        Message message1 = mappedFile.loadMessage(4, size);
        System.out.println(message1);
    }
}