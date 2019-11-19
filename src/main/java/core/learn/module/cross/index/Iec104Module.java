package core.learn.module.cross.index;

import core.learn.field.CommaddrField;
import core.learn.field.InfoaddrField;
import core.learn.module.Module;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import util.StringUtil;

import java.util.*;

public class Iec104Module extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_iec104");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> commaddrField;
                    List<String> infoaddrField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        commaddrField = new ArrayList<>();
                        infoaddrField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            commaddrField.add(StringUtil.trans(map.get("i_commaddr")));
                            infoaddrField.add(StringUtil.trans(map.get("i_infoaddr")));
                        }
                        CommaddrField.append(commaddrField);
                        InfoaddrField.append(infoaddrField);
                        return null;
                    }
                }
        ).collect();
    }
}
