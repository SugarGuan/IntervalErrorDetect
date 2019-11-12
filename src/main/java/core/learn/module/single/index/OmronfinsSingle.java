package core.learn.module.single.index;

import core.learn.module.Module;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Map;
//import java.util.List;
//import java.util.HashMap;
//import java.util.ArrayList;

public class OmronfinsSingle extends Module {

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
