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
     * @param redis : 报警用redis
     * @param subOPs : 子操作串String格式，使用“,”分割.
     * @param startTime : 存有异常记录的发生时间戳
     * @param endTime : 存有异常记录检测到的时间戳
     */
    private void report(Redis redis, String field, List<String> subOPs, Long occurCounting, Long startTime, Long endTime) {
        if (redis == null)
            return;
        if (subOPs == null)
            return;
        String json = convertJsonString(field, subOPs, occurCounting, startTime, endTime);
        redis.insertRedisList("ad_log", "ad_log_cycledetect|" + json);
        System.out.println("ad_log_cycledetect|" + json);
    }

    void report(Redis redis, String subOPs, Long startTime, Long endTime) {
        if (redis == null)
            return;
        if (subOPs == null)
            return;
        if (subOPs.length() < 1)
            return;
        String json = convertJsonString(subOPs, startTime, endTime);
        redis.insertRedisList("ad_log", "ad_log_cycledetect|" + json);
    }



    private String convertJsonString (String field, List<String> subOPs, Long occurCounting ,Long startTime, Long endTime) {
        JSONObject jsonBean = new JSONObject();
        jsonBean.put("i_mod", "ad");
        jsonBean.put("i_type", "cycledetect");
        jsonBean.put("i_start_time", Time.dateTimeFormat(startTime));
        jsonBean.put("i_end_time,", Time.dateTimeFormat(endTime));
        jsonBean.put("i_field",field);
        jsonBean.put("i_seq", rebuiltOperationList(subOPs));
        jsonBean.put("i_frequent", 10);
        jsonBean.put("i_occur", occurCounting);
        return jsonBean.toJSONString();
    }

    /**
     * convertJsonString方法用于将有用的报警信息(数据)转换成json语句
     * @param subOPs :操作队列（已处理过的形式的String类）
     * @param startTime : 存有异常记录的发生时间戳
     * @param endTime : 存有异常记录检测到的时间戳
     * @return : 一条json格式的报警信息，其中时间戳格式是yyyy-MM-dd HH:mm:ss
     */
    private String convertJsonString (String subOPs, Long startTime, Long endTime) {
        JSONObject jsonBean = new JSONObject();
        jsonBean.put("i_mod", "ad");
        jsonBean.put("i_type", "cycledetect");
        jsonBean.put("i_start_time", Time.dateTimeFormat(startTime));
        jsonBean.put("i_end_time,", Time.dateTimeFormat(endTime));
//        jsonBean.put("i_seq", rebuiltOperationList(subOPs));
        jsonBean.put("i_frequent", 10);
        return jsonBean.toJSONString();
    }

    /**
     * rebuiltOperationList()
     * 将操作队列构造成一个插入redis的报告String形式 操作队列字符串
     * @param subOPs 操作队列String组
     * @return 操作队列拼接String
     */
    private String rebuiltOperationList(List<String> subOPs) {
        if (subOPs.size() < 1)
            return "";
        StringBuffer sb = new StringBuffer();
        if (subOPs.size() < 11) {
            for (String op : subOPs) {
                sb.append(op);
                sb.append(",");
            }
            sb.deleteCharAt(sb.length() - 1); //删除最后一个逗号
            return sb.toString();
        }
        int i = 0;
        for ( ; i < 10 ; i++) {
            sb.append(subOPs.get(i));
            sb.append(",");
        }
        sb.deleteCharAt(sb.length()-1);
        sb.append("...");
        return sb.toString();
    }

    public void superReport(Map<String, List<Map<List<String>, Long>>> result, Long startTime, Redis redis) {
        if (result == null)
            return;
        if (result.size() == 0)
            return;

        Long start = Time.now();
        int i = 0;
        for (String field : result.keySet()) {
            if (Thread.currentThread().isInterrupted())
                return;
            for (Map<List<String>, Long> opCount : result.get(field)){
                if (Thread.currentThread().isInterrupted())
                    return;
                for (List<String> op : opCount.keySet())
                    if (i < 10) {
                        i++;
                        if (Thread.currentThread().isInterrupted())
                            return;
                        report(redis , field , op , opCount.get(op), startTime, Time.now());
                    } else {
                        try {
                            if (Time.now() - start < (60000)) {
                                Thread.sleep(Time.now() - start);
                                i = 0;
                            }


                        } catch (InterruptedException e) {
                            // System do not interrupt any thing
                        }
                    }
            }
        }
    }



}
