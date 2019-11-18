package core.detect;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import util.Config;
import util.Time;

import java.io.*;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
public class FieldHotKeyDetector implements Serializable {

    /**
     * FieldHotKeyDetector类
     * 检测模式的核心功能类，用于检测字段序列是否存在异常
     * 异常队列由学习模式得到的规则文件确定.
     */

    Map<String, List<String>> indexField = Config.getElasticSearchIndexFieldDict();
    JavaPairRDD<String, Map<String, Object>> rdd;
    List<String> elasticIndices = Config.getElasticsearchIndices();
    List<String> fields;
    List<String> commandLists;
    Alerter alert;
    List<String> lib;



    public void detect(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap, Long detectStartTime) {
        /**
         *  检测函数逻辑：
         *  1. 初始化报警器
         *  2. 获取es index信息
         *  3. 获取es - field对应表
         *  4. 根据每个index，查询其拥有的field
         *  5. 由于获取RDD比较慢，检测线程中断位是否挂起
         *  6. Fields验空，如空则无需检测该索引
         *  7. RDD验空，如空则无需检测该RDD（记录数据）
         *  8. 创建一个不超过20长度的操作队列，每次插入一个操作码。如果超过操作队列20，自动移除首位。
         *  9. 操作队列的所有子队列（连续子队列）与结果集对比。
         *  10. 如果检测到意外，使用alerter对象发送警报信息。alerter对象是Alerter类的实例。
         *  11. 循环，发送警报或完成检测后，检测下一字段（或下一索引）。由于检测流程可能比较慢，每个检查操作按原子操作进行
         *  中断状态检测。一旦发现线程中断标识立刻返回。
         */
        alertSetter();
        for (String str : elasticIndices) {
            fields = indexField.get(str);
            if (Thread.currentThread().isInterrupted())
                return ;
            if (fields == null)
                continue;
            rdd = rddMap.get(str);
            if (rdd == null)
                continue ;
            if (rdd.count() == 0)
                continue ;
            for (String field : fields) {
                if (Thread.currentThread().isInterrupted())
                    return ;
                libSetter(field);
                rdd.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        commandLists = new ArrayList<>();
                        for (Map<String, Object> map : maps) {

                            if (commandLists.size() >=20)
                                commandLists.remove(0);

                            if (map.get(field) instanceof Long)
                                commandLists.add(Long.toString((Long) map.get(field)));
                            else if (map.get(field) instanceof String)
                                commandLists.add((String) map.get(field));
                            else
                                continue;

                            if (checkInfile(commandLists)) {
                                alert.report(
                                        str,
                                        map,
                                        detectStartTime,
                                        Time.now()
                                );
                            }
                        }
                        return null;
                    }
                }).collect();
            }
        }
    }

    private boolean checkInfile(List<String> list) {
        /**
         *  checkInfile逻辑
         *  传入list为操作码，最长不超过20
         *  比较子串是否与规则文本匹配
         */
        int listLength = list.size();
        int i = 0;
        if (listLength == 0)
            return false;
        StringBuffer sb = new StringBuffer();
        List<String> subList;
        while (i < listLength) {
            subList = list.subList(i, listLength);
            for (String str : subList) {
                sb.append(str);
                sb.append(",");
            }

            String s = sb.toString();
            sb.setLength(0);
            if (lib.contains(s))
                return true;
            i++;
        }
        return false;
    }

    private void alertSetter () {
        alert = new Alerter();
    }

    /**
     * 获取对应字段的规则文件，将规则文件转填入List容器
     * @param field
     */


    synchronized private void libSetter (String field) {
        lib = new ArrayList<>();
        if (field.length() < 3)
            return;
        try {
            File file = new File("D:\\Project\\2020\\dig-lib\\" + field.substring(2) + ".iedb");

            BufferedReader br = new BufferedReader(new FileReader(file));
            String strTemp;
            while(null != (strTemp = br.readLine())) {
                lib.add(strTemp);
            }
        } catch (Exception e) {

        }
    }
}
