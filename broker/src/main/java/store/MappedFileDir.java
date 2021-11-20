package store;

import java.io.File;

/**
 * @author Zexho
 * @date 2021/11/20 10:21 上午
 */
public class MappedFileDir {

    /**
     * commit文件夹路径 $HOME/jinx/commit
     */
    private static final String COMMIT_DIR_PATH = System.getProperty("user.home") + File.separator + "jinx" + File.separator + "commitlog";

    /**
     * 初始化时候创建文件夹
     */
    public static boolean init() {
        File file = new File(COMMIT_DIR_PATH);
        if (!file.exists()) {
            return file.mkdirs();
        }
        return true;
    }

}
