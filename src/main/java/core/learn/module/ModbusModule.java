package core.learn.module;

import core.learn.field.AddrField;
import core.learn.field.CmdField;
import core.learn.field.FuncField;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class ModbusModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_modbus");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> funcField;
                    List<String> addrField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        funcField = new ArrayList<>();
                        addrField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            funcField.add((String) map.get("i_func"));
                            addrField.add((String) map.get("i_addr"));
                        }
                        FuncField.append(funcField);
                        AddrField.append(addrField);
                        return null;
                    }
                }
        ).collect();
    }
}
