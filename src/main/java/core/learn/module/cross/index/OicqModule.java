package core.learn.module.cross.index;

import core.learn.field.CmdField;
import core.learn.field.QqidField;
import core.learn.module.Module;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import util.StringUtil;

import java.util.*;

public class OicqModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_oicq");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> cmdField;
                    List<String> qqidField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        cmdField = new ArrayList<>();
                        qqidField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            cmdField.add(StringUtil.trans(map.get("i_cmd")));
                            qqidField.add(StringUtil.trans(map.get("i_qqid")));
                        }
                        CmdField.append(cmdField);
                        QqidField.append(qqidField);
                        return null;
                    }
                }
        ).collect();
    }
}
