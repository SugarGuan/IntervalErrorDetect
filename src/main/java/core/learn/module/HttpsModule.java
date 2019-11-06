package core.learn.module;

import core.learn.field.CmdField;
import core.learn.field.RdnField;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class HttpsModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_https");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> rdnField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        rdnField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            rdnField.add((String) map.get("i_rdn"));
                        }
                        RdnField.append(rdnField);
                        return null;
                    }
                }
        ).collect();
    }
}
