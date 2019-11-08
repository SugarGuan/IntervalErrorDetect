package util;

import org.joda.time.DateTime;
import java.util.Date;

public class Time {
    public static Long getRealTime () {
        DateTime dateTime = new DateTime(new Date());
        return dateTime.getMillis();
    }

    public static Long now () {
        return getRealTime();
    }

    public static String timeFormatChinese(Long timestamp) {
        Long second = timestamp / 1000;
        if (second < 100)
            return timestamp / 1000.0 + " 秒";
        Long minute = timestamp / 60000;
        if(minute < 60)
            return minute + " 分" + (timestamp - 60 * minute ) / 1000.0 + " 秒";
        return timestamp / 1000.0 + " 秒";
    }

    public static String timeFormatEnglish(Long timestamp) {
        Long second = timestamp / 1000;
        if (second < 100)
            return timestamp / 1000.0 + " second(s)";
        Long minute = timestamp / 60000;
        if(minute < 60)
            return minute + " minute " + (timestamp - 60 * minute * 1000) / 1000.0 + " second(s)";
        return timestamp / 1000.0 + " second(s)";
    }

}
