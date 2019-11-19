package core.learn.module.cross.index;

import core.learn.field.FunctionField;
import core.learn.module.Module;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import util.StringUtil;

import java.util.*;

public class S7plusModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_s7plus");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> functionField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        functionField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            functionField.add(StringUtil.trans(map.get("i_function")));
                        }
                        FunctionField.append(functionField);
                        return null;
                    }
                }
        ).collect();
    }
}
