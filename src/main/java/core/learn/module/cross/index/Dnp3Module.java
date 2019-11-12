package core.learn.module.cross.index;

import core.learn.field.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class Dnp3Module extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_dnp3");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> saddrField;
                    List<String> daddrField;
                    List<String> funcField;
                    List<String> groupnumField;
                    List<String> varnumField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        saddrField = new ArrayList<>();
                        daddrField = new ArrayList<>();
                        funcField = new ArrayList<>();
                        groupnumField = new ArrayList<>();
                        varnumField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            saddrField.add((String) map.get("i_saddr"));
                            daddrField.add((String) map.get("i_daddr"));
                            funcField.add((String) map.get("i_func"));
                            groupnumField.add((String) map.get("i_groupnum"));
                            varnumField.add((String) map.get("i_varnum"));
                        }
                        SaddrField.append(saddrField);
                        DaddrField.append(daddrField);
                        FuncField.append(funcField);
                        GroupnumField.append(groupnumField);
                        VarnumField.append(varnumField);
                        return null;
                    }
        }).collect();
    }
}
