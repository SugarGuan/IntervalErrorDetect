package core.learn.module.single.index;

import core.learn.field.*;
import core.learn.module.cross.index.Module;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class Eplv1Single extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_eplv1");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

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
                            String service = (String) map.get("i_service");
                            String dest = Long.toString((Long) map.get("i_dest"));
                            String source = Long.toString((Long) map.get("i_source"));
                            String cmd = (String) map.get("i_cmd");

                            if (serviceField.contains(service)) {
                                ServiceField.append(serviceField);
                                serviceField = new ArrayList<>();
                            }
                            if (destField.contains(dest)) {
                                DestField.append(destField);
                                destField = new ArrayList<>();
                            }
                            if (sourceField.contains(source)) {
                                SourceField.append(sourceField);
                                sourceField = new ArrayList<>();
                            }
                            if (cmdField.contains(cmd)) {
                                CmdField.append(cmdField);
                                cmdField = new ArrayList<>();
                            }

                            serviceField.add(service);
                            destField.add(dest);
                            sourceField.add(source);
                            cmdField.add(cmd);
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
