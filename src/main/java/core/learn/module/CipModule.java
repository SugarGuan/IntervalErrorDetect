package core.learn.module;

import core.learn.field.CmdField;
import core.learn.field.Pkt_typeField;
import core.learn.field.ServiceField;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class CipModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_cip");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> pktField;
                    List<String> serviceField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        pktField = new ArrayList<>();
                        serviceField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            pktField.add((String) map.get("pkt_type"));
                            serviceField.add((String) map.get("i_service"));
                        }
                        Pkt_typeField.append(pktField);
                        ServiceField.append(serviceField);
                        return null;
                    }
                }
        ).collect();
    }
}
