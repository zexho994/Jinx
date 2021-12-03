package store;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
}