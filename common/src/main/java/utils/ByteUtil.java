package utils;

import lombok.extern.log4j.Log4j2;

import java.io.*;

/**
 * @author Zexho
 * @date 2021/12/3 7:44 下午
 */
@Log4j2
public class ByteUtil {

    public static byte[] to(Object obj) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(obj);
            return bos.toByteArray();
        } catch (IOException e) {
            log.warn(e);
            return null;
        }
    }

    public static Object to(byte[] bytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes); ObjectInputStream in = new ObjectInputStream(bis)) {
            return in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            log.error(e);
            return null;
        }
    }

}
