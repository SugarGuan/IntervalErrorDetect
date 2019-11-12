package core.learn.module.cross.index;

import core.learn.field.TypeField;
import core.learn.module.Module;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class MmsModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_mms");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> typeField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        typeField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            typeField.add((String) map.get("i_type"));
                        }
                        TypeField.append(typeField);
                        return null;
                    }
                }
        ).collect();
    }
}
