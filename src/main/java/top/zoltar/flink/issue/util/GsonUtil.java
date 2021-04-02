package top.zoltar.flink.issue.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.lang.reflect.Type;

/**
 * Created by wangfeifan on 2021/4/1.
 */

public class GsonUtil {
    final private static Gson gson;

    static {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gson = gsonBuilder.create();
    }

    public static Gson getGson(){
        return gson;
    }

    public static <T> T fromJson(String jsonStr, Class<T> clazz) {
        return gson.fromJson(jsonStr, clazz);
    }

    public static <T> T fromJson(String jsonStr, Type type) {
        return gson.fromJson(jsonStr, type);
    }

    public static <T> T fromJson(JsonElement json, Type type) {
        return gson.fromJson(json, type);
    }

    public static JsonElement toJsonTree(String jsonStr){
        return gson.toJsonTree(jsonStr);
    }

    public static JsonObject toJsonObject(String jsonStr){
        return gson.fromJson(jsonStr, JsonObject.class);
    }

    public static String toJson(Object obj){
        return gson.toJson(obj);
    }

}

