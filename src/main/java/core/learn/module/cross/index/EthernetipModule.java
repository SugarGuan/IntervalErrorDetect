package core.learn.module.cross.index;

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

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> cmdField;
                    List<String> pkt_typeField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        cmdField = new ArrayList<>();
                        pkt_typeField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            String cmd = (String) map.get("i_cmd");
                            String pkt_type = (String) map.get("pkt_type");

                            if (cmdField.contains(cmd)){
                                CmdField.append(cmdField);
                                cmdField = new ArrayList<>();
                            }

                            if (pkt_typeField.contains(pkt_type)) {
                                Pkt_typeField.append(pkt_typeField);
                                pkt_typeField = new ArrayList<>();
                            }

                            cmdField.add(cmd);
                            pkt_typeField.add(pkt_type);
                        }
                        CmdField.append(cmdField);
                        Pkt_typeField.append(pkt_typeField);
                        return null;
                    }
                }
        ).collect();
    }
}
