package utils;

import lombok.extern.log4j.Log4j2;
import message.Message;

import java.io.*;

/**
 * @author Zexho
 * @date 2021/12/3 7:44 下午
 */
@Log4j2
public class ByteUtil {

    public static byte[] to(int num) {
        return new byte[]{
                (byte) (num >>> 24),
                (byte) (num >>> 16),
                (byte) (num >>> 8),
                (byte) num};
    }

    public static byte[] to(long num) {
        return new byte[]{
                (byte) (num >> 56),
                (byte) (num >> 48),
                (byte) (num >> 40),
                (byte) (num >> 32),
                (byte) (num >> 24),
                (byte) (num >> 16),
                (byte) (num >> 8),
                (byte) num};
    }

    public static byte[] to(Object obj) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(obj);
            return bos.toByteArray();
        } catch (IOException e) {
            log.warn(e);
            return new byte[0];
        }
    }

    public static <T> T to(byte[] bytes, Class<T> t) {
        if (bytes == null) {
            return null;
        }

        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes); ObjectInputStream in = new ObjectInputStream(bis)) {
            return (T) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            log.error(e);
            return null;
        }
    }

    public static Message toMessage(byte[] bytes) {
        return to(bytes, Message.class);
    }
}
