package core.learn.module.cross.index;

import core.learn.field.ProtoField;
import core.learn.module.Module;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import util.StringUtil;

import java.util.*;

public class Sf_haspModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_sf_hasp");
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
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        protoField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            protoField.add(StringUtil.trans(map.get("i_proto")));
                        }
                        ProtoField.append(protoField);
                        return null;
                    }
                }
        ).collect();
    }
}
