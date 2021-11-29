package store;

import common.MemoryCapacity;
import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

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

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileSize = fileSize;
        this.fileName = fileName;
        String filePath = Commitlog.FOLDER_COMMIT.getAbsolutePath() + File.separator + fileName + ".jinx";
        this.file = new File(filePath);
        this.fileFormOffset = Long.parseLong(fileName);

        ensureDirOk(file.getParent());

        boolean initSuccess = false;
        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
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
        this.fileChannel.force(false);
    }
}
