package store;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

class MappedFileTest {

    @Test
    void load() {
        try {
            MappedFile mappedFile = new MappedFile("0", Commitlog.DEFAULT_MAPPED_FILE_SIZE);
            List<InnerMessage> load = mappedFile.load();
            Assertions.assertTrue(load.size() > 0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}