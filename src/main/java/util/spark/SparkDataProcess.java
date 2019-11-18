package util.spark;

import util.Config;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

public class SparkDataProcess implements Serializable {

    /**
     *  SparkDataProcess 类
     *  提供各种Spark RDD对象数据的处理功能，包括：
     *  pairRDD-RDD互相转换方法
     *  根据应用需求（根据时间段、IP地址或MAC地址确定单一会话组）重设PairRDD key方法
     *  根据key合并pairRDD，将记录合并成List后合并为一个RDD记录的方法
     *  获取rdd记录数的方法
     */


    /**
     * setPairRDDtoRDD()
     * 方法将PairRDD转换为RDD，转换的方式是取RDD的value，抛弃key值。
     * es中拉取出的key可能没有分析意义，本插件中提供本功能去掉key。
     * @param pairRDD 待转换的pairRDD
     * @return
     */
    public JavaRDD<Map<String, Object>> setPairRDDToRDD(JavaPairRDD<String, Map<String, Object>> pairRDD) {
        return pairRDD.values();
    }

    /**
     * setRDDtoPairRDD()
     * 根据业务需要重设PairRDD key。 这里将已经去掉无用key的RDD重新膨胀为pairRDD
     * 将设备名(i_dname)、IP地址(i_ipsrc + i_ipdst) 或 MAC地址(i_macsrc + i_macdst)组合成字符串后设置为重组pairRDD的key
     * 其中，不同索引RDD具体是由IP地址还是MAC地址确定会话的 表格信息 被保存在Config中。
     * @param index 索引名
     * @param RDD 带操作的RDD
     * @return 重设键的pair - RDD （可以为空）
     */
    public JavaPairRDD<String, Map<String, Object>> setRDDtoPairRDD(String index, JavaRDD<Map<String, Object>> RDD) {
        int indexType = Config.getDialogIndexMapValue(index);
        try {

            if (indexType == 1) {
                return RDD.mapToPair(new PairFunction<Map<String, Object>, String, Map<String, Object>>() {
                    StringBuffer key;
                    @Override
                    public Tuple2<String, Map<String, Object>> call(Map<String, Object> stringObjectMap) throws Exception {
                        key = new StringBuffer();
                        key.append((String) stringObjectMap.get("i_dname"));
                        key.append((String) stringObjectMap.get("i_ipsrc"));
                        key.append((String) stringObjectMap.get("i_ipdst"));
                        return new Tuple2<String, Map<String, Object>>((String) key.toString(), stringObjectMap);
                    }
                });
            }

            if (indexType == 2) {
                return RDD.mapToPair(new PairFunction<Map<String, Object>, String, Map<String, Object>>() {
                    StringBuffer key;
                    @Override
                    public Tuple2<String, Map<String, Object>> call(Map<String, Object> stringObjectMap) throws Exception {
                        key = new StringBuffer();
                        key.append((String) stringObjectMap.get("i_dname"));
                        key.append((String) stringObjectMap.get("i_macsrc"));
                        key.append((String) stringObjectMap.get("i_macdst"));
                        return new Tuple2<String, Map<String, Object>>(key.toString(), stringObjectMap);
                    }
                });
            }

            return null;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * resetJavaPairRDD()
     * 一键转换RDD的方法。作为桥方法，包装了去掉key和转换pairRDD的两个流程。
     * @param index 索引名
     * @param rdd RDD
     * @return 重新设置key 的pairRDD
     */
    public JavaPairRDD<String, Map<String, Object>> resetJavaPairRDD(String index, JavaPairRDD<String, Map<String, Object>> rdd) {
        return(setRDDtoPairRDD(index, setPairRDDToRDD(rdd)));
    }

    /**
     * combineDialogJavaPairRDD()
     * 根据PairRDD的key值合并PairRDD记录为PairRDD记录集，减少记录数量。
     * @param rdd 待合并的pairRDD
     * @return 根据key值合并后的Pair RDD
     */
    public JavaPairRDD<String, Iterable<Map<String, Object>>> combineDialogJavaPairRDD (JavaPairRDD<String, Map<String, Object>> rdd) {
        return rdd.groupByKey();
    }

    /**
     * getCountOfRDD()  （暂未启用）
     * 返回RDD结果数量
     * 查询RDD结果数的同时记录查询次数，由于存在ES数据长时间不达学习标准记录下限的可能性。
     * @param RDD 待检查的RDD
     * @return RDD记录数
     */
    public Long getCountOfRDD(JavaRDD<Map<String, Object>> RDD) {
        if (RDD == null)
            return 0L;
        return RDD.count();
    }

    /**
     * getCountOfRDD()  （暂未启用）
     * @param RDD 待检查的RDD
     * @return RDD记录数
     */
    public Long getCountOfRDD(JavaPairRDD<String, Map<String, Object>> RDD) {
        if (RDD == null)
            return 0L;
        return RDD.count();
    }
}
