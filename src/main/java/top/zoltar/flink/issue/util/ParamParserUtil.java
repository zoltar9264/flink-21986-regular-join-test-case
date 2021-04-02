package top.zoltar.flink.issue.util;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wangfeifan on 2021/4/1.
 */


public class ParamParserUtil {
    final private static Logger logger = LoggerFactory.getLogger(ParamParserUtil.class);


    /**
     * parser a time duration string to time in milliseconds
     * egg: 5m, 1d, 3h
     * unit:
     *      d = day
     *      h = hour
     *      m = minute
     *      s = second
     *      ms = millisecond
     * @param str
     * @return time in milliseconds
     */
    public static long parseTime(String str){
        String pattern = "\\d+(d|h|m|s|ms)";
        if( ! str.matches(pattern) ) throw new RuntimeException("bad time format :" + str);

        long size = Long.valueOf(str.replaceAll("[^0-9]", ""));

        if( str.contains("ms") ){
            return Time.milliseconds(size).toMilliseconds();
        } else if( str.contains("s") ) {
            return Time.seconds(size).toMilliseconds();
        } else if( str.contains("m") ) {
            return Time.minutes(size).toMilliseconds();
        } else if( str.contains("h") ) {
            return Time.hours(size).toMilliseconds();
        } else {
            return Time.days(size).toMilliseconds();
        }
    }


    /**
     * parser a memory size string to memory size in bytes
     * egg: 1B, 1G, 4k
     * unit:
     *      B = byte
     *      k|K = kilobyte
     *      m|M = megabyte
     *      g|G = gigabyte
     * @param str
     * @return memory size in bytes
     */
    public static int parseMem(String str){
        String pattern = "\\d+(B|k|K|m|M|g|G)";
        if( ! str.matches(pattern) ) throw new RuntimeException("bad mem format :" + str);

        int size = Integer.valueOf(str.replaceAll("[^0-9]", ""));

        if( str.contains("k") || str.contains("K") ){
            return size * 1024;
        } else if ( str.contains("m") || str.contains("M") ) {
            return size * 1024 * 1024;
        } else if ( str.contains("g") || str.contains("G") ) {
            return size * 1024 * 1024 * 1024;
        } else {
            return size;
        }
    }

}
