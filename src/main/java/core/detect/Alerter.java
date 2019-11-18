package core.detect;

import com.alibaba.fastjson.JSONObject;
import dao.redis.Redis;
import util.Config;
import util.Time;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class Alerter implements Serializable {

    /**
     *   Alerter 类
     *   该类是检测模式(Detect Mode)中用于发送报警信息的类。
     *   report方法用于向指定的redis表插入一条报警记录
     *   convertJsonString方法用于将有用的报警信息(数据)转换成json语句
     *
     *   报警功能在检测中使用，检测依赖spark操作，所以本类需要实现序列化接口
     *   如果有其他警报方式，可以直接在本类中进行扩展
     */


    /**
     * report方法用于向指定的redis表插入一条报警记录
     * @param tableName : 代表检测出问题的index (ElasticSearch索引名)
     * @param map : 存有异常被检测到时对应的记录字段
     * @param startTime : 存有异常记录的发生时间戳
     * @param endTime : 存有异常记录检测到的时间戳
     */
    public void report(String tableName, Map<String, Object> map, Long startTime, Long endTime) {
        if (tableName == null)
            tableName = "null";
        String json = convertJsonString(map, startTime, endTime);
        Redis redis = new Redis();
        redis.insertRedisList("ad_log", tableName + json);
    }

    /**
     * convertJsonString方法用于将有用的报警信息(数据)转换成json语句
     * @param map : 存有异常被检测到时对应的记录字段
     * @param startTime : 存有异常记录的发生时间戳
     * @param endTime : 存有异常记录检测到的时间戳
     * @return : 一条json格式的报警信息，其中时间戳格式是yyyy-MM-dd HH:mm:ss
     */
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
