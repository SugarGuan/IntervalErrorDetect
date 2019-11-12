package core.learn.module;

import core.learn.field.CmdField;
import core.learn.field.InterfaceField;
import core.learn.field.MethodField;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class OpcModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_opc");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> interfaceField;
                    List<String> methodField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        interfaceField = new ArrayList<>();
                        methodField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            interfaceField.add((String) map.get("i_interface"));
                            methodField.add((String) map.get("i_method"));
                        }
                        InterfaceField.append(interfaceField);
                        MethodField.append(methodField);
                        return null;
                    }
                }
        ).collect();
    }
}
