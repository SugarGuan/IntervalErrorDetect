package core.learn.module.single.index;

import core.learn.field.*;
import core.learn.module.Module;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class FfhseSingle extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_ffhse");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> protoField;
                    List<String> msgtypeField;
                    List<String> confirmField;
                    List<String> serviceField;
                    List<String> fda_addrField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        protoField = new ArrayList<>();
                        msgtypeField = new ArrayList<>();
                        confirmField = new ArrayList<>();
                        serviceField = new ArrayList<>();
                        fda_addrField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            protoField.add((String) map.get("i_proto"));
                            msgtypeField.add((String) map.get("i_msgtype"));
                            confirmField.add((String) map.get("i_confirm"));
                            serviceField.add((String) map.get("i_service"));
                            fda_addrField.add((String) map.get("i_fda_addr"));
                        }
                        ProtoField.append(protoField);
                        MsgtypeField.append(msgtypeField);
                        ConfirmField.append(confirmField);
                        ServiceField.append(serviceField);
                        Fda_addrField.append(fda_addrField);
                        return null;
                    }
                }
        ).collect();
    }
}
