package util.Spark;

import org.codehaus.janino.Java;
import util.Config;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public JavaRDD<Map<String, Object>> resetDialogIndexRDD(String index, JavaRDD<Map<String, Object>> RDD) {
        int resetDialogIndexRDDmode = Config.getDialogIndexMapValue(index);
        if (resetDialogIndexRDDmode == 1)
            return resetDialogIndexRDDasAppIPaddr(RDD);
        if (resetDialogIndexRDDmode == 2)
            return resetDialogIndexRDDasAppMACaddr(RDD);
        return null;
    }

    private JavaRDD<Map<String, Object>> resetDialogIndexRDDasAppIPaddr(JavaRDD<Map<String, Object>> RDD) {

        try {
            JavaRDD<Map<String, Object>> resetRDD = RDD.map(
                    new Function<Map<String, Object>, Map<String, Object>>() {
                        Map<String, Object> hashMap;
                        @Override
                        public Map<String, Object> call(Map<String, Object> stringObjectMap) throws Exception {
                            hashMap = new HashMap<>();
                            hashMap.put(
                                    (String)stringObjectMap.get("i_dname") +
                                            (String)stringObjectMap.get("i_ipsrc") +
                                            (String)stringObjectMap.get("i_ipdst"),
                                    stringObjectMap
                            );
                            return hashMap;
                        }
                    }
            );
            return resetRDD;
        } catch (Exception e) {
            return null;
        }
    }

    private JavaRDD<Map<String, Object>> resetDialogIndexRDDasAppMACaddr(JavaRDD<Map<String, Object>> RDD) {

        try {
            JavaRDD<Map<String, Object>> resetRDD = RDD.map(
                    new Function<Map<String, Object>, Map<String, Object>>() {
                        Map<String, Object> hashMap;
                        @Override
                        public Map<String, Object> call(Map<String, Object> stringObjectMap) throws Exception {
                            hashMap = new HashMap<>();
                            hashMap.put(
                                    (String)stringObjectMap.get("i_dname") +
                                            (String)stringObjectMap.get("i_macsrc") +
                                            (String)stringObjectMap.get("i_macdst") ,
                                    stringObjectMap
                            );
                            return hashMap;
                        }
                    }
            );
            return resetRDD;
        } catch (Exception e) {
            return null;
        }
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
