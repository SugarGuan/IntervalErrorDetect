package core.detect;

import com.alibaba.fastjson.JSONObject;
import dao.redis.Redis;
import util.Config;
import util.Time;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class Alerter implements Serializable {
    public void report(String tableName, Map<String, Object> map, Long startTime, Long endTime) {
        if (tableName == null)
            tableName = "null";
        String json = convertJsonString(map, startTime, endTime);
        Redis redis = new Redis();
        redis.insertRedisList("ad_log", tableName + json);
    }

    private String convertJsonString (Map<String, Object> map, Long startTime, Long endTime) {
        JSONObject jsonBean = new JSONObject();
        jsonBean.put("i_mod", "ad");
        jsonBean.put("i_type", "IntervalErrorDetect");
        jsonBean.put("i_start_time", Time.dateTimeFormat(startTime));
        jsonBean.put("i_end_time,", Time.dateTimeFormat(endTime));
        if (map == null) {
            jsonBean.put("i_dname", "null");
            return jsonBean.toJSONString();
        }
        // else
        List<String> indexField = Config.getFields();
        for (String str: indexField) {
            if (map.get(str) != null) {
                if (map.get(str) instanceof String) {
                    jsonBean.put(str, (String) map.get(str));
                }
                if (map.get(str) instanceof Long) {
                    jsonBean.put(str, Long.toString((Long) map.get(str)));
                }
            }
        }
        return jsonBean.toJSONString();
    }
}
