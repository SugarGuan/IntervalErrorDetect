package core.learn.module;

import core.learn.field.CmdField;
import core.learn.field.Pkt_typeField;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class EthernetipModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_ethernetip");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> cmdField;
                    List<String> pkt_type;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        cmdField = new ArrayList<>();
                        pkt_type = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            cmdField.add((String) map.get("i_cmd"));
                            pkt_type.add((String) map.get("pkt_type"));
                        }
                        CmdField.append(cmdField);
                        Pkt_typeField.append(pkt_type);
                        return null;
                    }
                }
        ).collect();
    }
}
