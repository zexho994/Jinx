package utils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

class ByteUtilTest {

    String fileName = "byte_test.txt";
    File file = new File(fileName);

    {
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void test() throws IOException {
        ByteUtilTest test = new ByteUtilTest();

        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        test.writeLong(10, randomAccessFile);
        test.writeLong(11, randomAccessFile);
        test.writeLong(12, randomAccessFile);
        test.writeLong(13, randomAccessFile);


        RandomAccessFile readAccess = new RandomAccessFile(file, "rw");
        assert test.readLong(readAccess) == 10;
        assert test.readLong(readAccess) == 11;
        assert test.readLong(readAccess) == 12;
        assert test.readLong(readAccess) == 13;
    }

    @Test
    public void test2() {

    }

    @Test
    void writeLong(long n, RandomAccessFile randomAccessFile) throws IOException {
        randomAccessFile.writeLong(n);
    }

    @Test
    void writeLong() {
    }

    @Test
    long readLong(RandomAccessFile randomAccessFile) throws IOException {
        return randomAccessFile.readLong();
    }

    @Test
    public void getString() {
        try {
            final int size = 18;
            MappedByteBuffer inputBuffer = new RandomAccessFile(file, "r")
                    .getChannel().map(FileChannel.MapMode.READ_ONLY, 0, size);
            byte[] bs = new byte[inputBuffer.capacity()];
            for (int offset = 0; offset < inputBuffer.capacity(); offset++) {
                bs[offset] = inputBuffer.get(offset);
            }
            String str = new String(bs);
            System.out.println(str);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}