package utils;

/**
 * @author Zexho
 * @date 2021/12/6 5:25 下午
 */
public class ArrayUtils {

    public static byte[] merge(byte[] arr1, byte[] arr2) {
        byte[] arr = new byte[arr1.length + arr2.length];
        System.arraycopy(arr1, 0, arr, 0, arr1.length);
        System.arraycopy(arr2, 0, arr, arr1.length, arr2.length);
        return arr;
    }

}
