package util;

import org.joda.time.DateTime;
import java.util.Date;

public class Time {
    private static Long lastTime = 0L;
    public static Long getRealTime () {
        DateTime dateTime = new DateTime(new Date());
        return dateTime.getMillis();
    }

    public static Long now () {
        return getRealTime();
    }

    public static Long getLastTime() {
        return lastTime;
    }

    public static void SetLastTime(Long time) {
        lastTime = time;
    }

}
