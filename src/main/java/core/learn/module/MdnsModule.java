package core.learn.module;

import core.learn.field.CmdField;
import core.learn.field.HostnameField;
import core.learn.field.IpField;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class MdnsModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_mdns");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> ipField;
                    List<String> hostnameField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        ipField = new ArrayList<>();
                        hostnameField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            ipField.add((String) map.get("i_ip"));
                            hostnameField.add((String) map.get("i_hostname"));
                        }
                        IpField.append(ipField);
                        HostnameField.append(hostnameField);
                        return null;
                    }
                }
        ).collect();
    }
}
