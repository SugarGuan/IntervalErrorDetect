package core.learn.module;

import core.learn.field.CmdField;
import core.learn.field.DestField;
import core.learn.field.ServiceField;
import core.learn.field.SourceField;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class Eplv1Module extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_eplv1");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> serviceField;
                    List<String> destField;
                    List<String> sourceField;
                    List<String> cmdField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        serviceField = new ArrayList<>();
                        destField = new ArrayList<>();
                        sourceField = new ArrayList<>();
                        cmdField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            serviceField.add((String) map.get("i_service"));
                            destField.add((String) map.get("i_dest"));
                            sourceField.add((String) map.get("i_source"));
                            cmdField.add((String) map.get("i_cmd"));
                        }
                        ServiceField.append(serviceField);
                        DestField.append(destField);
                        SourceField.append(sourceField);
                        CmdField.append(cmdField);
                        return null;
                    }
                }
        ).collect();
    }
}
