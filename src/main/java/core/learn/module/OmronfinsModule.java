package core.learn.module;

import core.learn.field.CmdField;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class OmronfinsModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_omronfins");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
//        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);
//
//        if (pairRDD == null)
//            return ;
//
//        if (pairRDD.count() == 0)
//            return ;
//
//        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
//                    List<String> cmdField;
//                    @Override
//                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
//                        cmdField = new ArrayList<>();
//                        for (Map<String, Object> map: maps) {
//                            cmdField.add((String) map.get("i_cmd"));
//                        }
//                        CmdField.append(cmdField);
//                        return null;
//                    }
//                }
//        ).collect();
    }
}
