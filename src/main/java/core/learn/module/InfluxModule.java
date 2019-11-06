package core.learn.module;

import core.learn.field.CmdField;
import core.learn.field.Pkt_typeField;
import core.learn.field.UrlField;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class InfluxModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_influx");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> pkt_typeField;
                    List<String> urlField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        pkt_typeField = new ArrayList<>();
                        urlField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            pkt_typeField.add((String) map.get("i_pkt_type"));
                            urlField.add((String) map.get("i_url"));
                        }
                        Pkt_typeField.append(pkt_typeField);
                        UrlField.append(urlField);
                        return null;
                    }
                }
        ).collect();
    }
}
