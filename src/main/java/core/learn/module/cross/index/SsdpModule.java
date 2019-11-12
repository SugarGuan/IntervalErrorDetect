package core.learn.module.cross.index;

import core.learn.field.*;
import core.learn.module.Module;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class SsdpModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_ssdp");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> pkt_typeField;
                    List<String> ipField;
                    List<String> locField;
                    List<String> serverField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        pkt_typeField = new ArrayList<>();
                        ipField = new ArrayList<>();
                        locField = new ArrayList<>();
                        serverField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            pkt_typeField.add((String) map.get("i_pkt_type"));
                            ipField.add((String) map.get("i_ip"));
                            locField.add((String) map.get("i_loc"));
                            serverField.add((String) map.get("i_server"));
                        }
                        Pkt_typeField.append(pkt_typeField);
                        IpField.append(ipField);
                        LocField.append(locField);
                        ServerField.append(serverField);
                        return null;
                    }
                }
        ).collect();
    }
}
