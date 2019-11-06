package core.learn.module;

import core.learn.field.CmdField;
import core.learn.field.Data_typeField;
import core.learn.field.FuncField;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class BacnetModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_bacnet");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> data_typeField;
                    List<String> funcField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        data_typeField = new ArrayList<>();
                        funcField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            data_typeField.add((String) map.get("i_data_type"));
                            funcField.add((String) map.get("i_func"));
                        }
                        Data_typeField.append(data_typeField);
                        FuncField.append(funcField);
                        return null;
                    }
                }
        ).collect();
    }
}
