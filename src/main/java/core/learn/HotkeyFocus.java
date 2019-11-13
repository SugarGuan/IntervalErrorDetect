package core.learn;

import org.apache.spark.api.java.JavaPairRDD;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

public class HotkeyFocus {
    public Map<String, String> getInfo(JavaPairRDD<String, Map<String, Object> > rdd, List<List<String>> hotKey) {
        if (hotKey == null)
            return null;
        Map<String, String> result = new HashMap<>();
        result.put("a","a");

        return result;

    }
}
