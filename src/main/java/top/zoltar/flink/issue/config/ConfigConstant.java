package top.zoltar.flink.issue.config;

/**
 * Created by wangfeifan on 2021/4/2.
 */

public class ConfigConstant {

    final public static int PER_SOURCE_SPEED = 200;

    final public static double JOIN_RATE = 0.35;

    final public static int TEXT1LENGTH = 1024;
    final public static int TEXT2LENGTH = 1024;

    final public static long IDLE_STATE_RETENTION = 24 * 60 * 60 * 1000;
    final public static String SEC_REC_DISTSEGS = "30m,0.8__90m,0.15__22h,0.05";

    // checkpoint config
    final public static long CK_INTERVAL = 3 * 60 * 1000;
    final public static long CK_MIN_PAUSE = 500;
    final public static long CK_TIMEOUT = 10 * 60 * 1000;
    final public static int CK_MAX_CONCURRENT = 1;
    final public static String CK_MODE = "EXACTLY_ONCE";
}
