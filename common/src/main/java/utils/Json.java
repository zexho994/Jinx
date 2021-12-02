package utils;

import com.google.gson.Gson;

/**
 * @author Zexho
 * @date 2021/12/2 4:38 下午
 */
public class Json {

    private static final Gson GSON = new Gson();

    public static String toJson(Object object) {
        return GSON.toJson(object);
    }

    public static String toJsonLine(Object object) {
        return toJson(object) + "\n";
    }

    public static <T> T fromJson(String json, Class<T> type) {
        return GSON.fromJson(json, type);
    }

}
