package util.spark;

import util.Config;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class SparkDataProcess implements Serializable {

    public JavaRDD<Map<String, Object>> setPairRDDToRDD(JavaPairRDD<String, Map<String, Object>> pairRDD) {
        return pairRDD.values();
    }

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

    public JavaPairRDD<String, Map<String, Object>> resetJavaPairRDD(String index, JavaPairRDD<String, Map<String, Object>> rdd) {
        return(setRDDtoPairRDD(index, setPairRDDToRDD(rdd)));
    }

    public JavaPairRDD<String, Iterable<Map<String, Object>>> combineDialogJavaPairRDD (JavaPairRDD<String, Map<String, Object>> rdd) {
        return rdd.groupByKey();
    }

    public Long getCountOfRDD(JavaRDD<Map<String, Object>> RDD) {
        if (RDD == null)
            return 0L;
        return RDD.count();
    }

    public Long getCountOfRDD(JavaPairRDD<String, Map<String, Object>> RDD) {
        if (RDD == null)
            return 0L;
        return RDD.count();
    }
}
