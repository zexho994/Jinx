package config;

import common.MemoryCapacity;

import java.io.File;

/**
 * @author Zexho
 * @date 2022/1/18 4:08 PM
 */
public class StoreConfig {

    public static String commitlogPath = System.getProperty("user.home") + File.separator + "jinx" + File.separator + "commitlog" + File.separator;
    public static int commitlogSize = 8 * MemoryCapacity.KB;

    public static String consumeQueuePath = System.getProperty("user.home") + File.separator + "jinx" + File.separator + "consumeQueue" + File.separator;
    public static int consumeQueueSize = MemoryCapacity.KB;

    public static String consumeOffsetPath = System.getProperty("user.home") + File.separator + "jinx" + File.separator + "consumeOffset" + File.separator;
    public static int consumeOffsetSize = MemoryCapacity.B * 8;

}
