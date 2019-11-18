package util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Date;

public class Time {
    /**
     * Time 类
     * 时间类，封装了部分插件所涉及的时间操作，包括：
     * 获取实时时间戳（ms单位，1970-01-01 00:00:00起）方法
     * 获取实时时间戳的桥方法 （由于部分方法使用时方法名调用异同故暂时保留了now()方法）
     * 将时间差转换为时间单位（中、英文）
     * 将时间戳转换为年月日格式（redis报警格式）
     */
    public static final String STANDRD_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * 获取实时时间戳
     * 1000ms = 1s
     * @return 实时时间戳（ms单位，1970-01-01 00:00:00起）
     */
    public static Long getRealTime () {
        DateTime dateTime = new DateTime(new Date());
        return dateTime.getMillis();
    }

    /**
     * now() (待改进的方法)
     * 获取实时时间戳的桥方法
     * 本方法的设计是考虑不同场景下代码增强可读性
     * 程序中存在部分混用情况，该方法会在后续版本和getRealTime方法统一为now()方法
     * @return 实时时间戳（ms单位，1970-01-01 00:00:00起）
     */
    public static Long now () {
        return getRealTime();
    }

    /**
     * timeFormatChinese()
     * 将时间戳转换为中文时间格式
     * @param timestamp 时间戳
     * @return 中文时间格式，如： 1 分 5 秒
     */
    public static String timeFormatChinese(Long timestamp) {
        Long second = timestamp / 1000;
        if (second < 100)
            return timestamp / 1000.0 + " 秒";
        Long minute = timestamp / 60000;
        if(minute < 60)
            return minute + " 分" + (timestamp - 60 * minute ) / 1000.0 + " 秒";
        return timestamp / 1000.0 + " 秒";
    }

    /**
     * timeFormatEnglish()
     * 将时间戳转换为英文时间格式 / Convert timestamps to English time format.
     * @param timestamp 时间戳 / Timestamps (The unit of milliseconds, from 0:00 on January 1, 1970)
     * @return 英文时间格式，如： 1 minute 5 seconds / English time format, such as: 1 minute 5 seconds
     */
    public static String timeFormatEnglish(Long timestamp) {
        Long second = timestamp / 1000;
        if (second < 100)
            return timestamp / 1000.0 + " second(s)";
        Long minute = timestamp / 60000;
        if(minute < 60)
            return minute + " minute " + (timestamp - 60 * minute * 1000) / 1000.0 + " second(s)";
        return timestamp / 1000.0 + " second(s)";
    }

    /**
     * dateTimeFormat()
     * 时间转换函数，将时间戳转换为检测模式报警插入redis记录中所需要的 yyyy-MM-dd HH:mm:ss 格式
     * @param timestamp 时间戳
     * @return yyyy-MM-dd HH:mm:ss 格式时间
     */
    public static String dateTimeFormat (Long timestamp) {
        DateTime dateTime = new DateTime(timestamp);
        return dateTime.toString(STANDRD_FORMAT);
    }

}
