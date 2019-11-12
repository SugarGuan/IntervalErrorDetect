package core.learn.module.single.index;

import core.learn.field.DomainField;
import core.learn.module.cross.index.Module;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class DnsSingle extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_dns");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> domainField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        domainField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            domainField.add((String) map.get("i_domain"));
                        }
                        DomainField.append(domainField);
                        return null;
                    }
                }
        ).collect();
    }
}
