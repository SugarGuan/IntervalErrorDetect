package core.learn.module.cross.index;

import core.learn.field.RdnField;
import core.learn.module.Module;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import util.StringUtil;

import java.util.*;

public class HttpsModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_https");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> rdnField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        rdnField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            String rdn = StringUtil.trans(map.get("i_rdn")) ;
                            if (rdnField.contains(rdn)) {
                                RdnField.append(rdnField);
                                rdnField = new ArrayList<>();
                            }

                            rdnField.add(rdn);
                        }
                        RdnField.append(rdnField);
                        return null;
                    }
                }
        ).collect();
    }
}
