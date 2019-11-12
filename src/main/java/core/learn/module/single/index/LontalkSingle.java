package core.learn.module.single.index;

import core.learn.field.*;
import core.learn.module.Module;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class LontalkSingle extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_lontalk");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> vendorField;
                    List<String> sidField;
                    List<String> subnet_sField;
                    List<String> subnet_dField;
                    List<String> node_sField;
                    List<String> node_dField;
                    List<String> domainField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        vendorField = new ArrayList<>();
                        sidField = new ArrayList<>();
                        subnet_sField = new ArrayList<>();
                        subnet_dField = new ArrayList<>();
                        node_sField = new ArrayList<>();
                        node_dField = new ArrayList<>();
                        domainField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            vendorField.add((String) map.get("i_vendor_code"));
                            sidField.add((String) map.get("i_sid"));
                            subnet_sField.add((String) map.get("i_subnet_s"));
                            subnet_dField.add((String) map.get("i_subnet_d"));
                            node_sField.add((String) map.get("i_node_s"));
                            node_dField.add((String) map.get("i_node_d"));
                            domainField.add((String) map.get("i_domain"));
                        }
                        Vendor_codeField.append(vendorField);
                        SidField.append(sidField);
                        Subnet_sField.append(subnet_sField);
                        Subnet_dField.append(subnet_dField);
                        Node_sField.append(node_sField);
                        Node_dField.append(node_dField);
                        DomainField.append(domainField);
                        return null;
                    }
                }
        ).collect();
    }
}
