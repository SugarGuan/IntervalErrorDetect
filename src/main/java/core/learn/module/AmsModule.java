package core.learn.module;

import util.Config;
import core.learn.HotkeyFinder;
import core.learn.field.CmdField;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class AmsModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_ams");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD.count() == 0)
            return ;

//        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
//                    List<String> cmdField;
//                    @Override
//                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
//                        cmdField = new ArrayList<>();
//                        for (Map<String, Object> map: maps) {
//                            String cmd = (String) map.get("i_cmd");
//                            if (cmdField.contains(cmd)){
//                                CmdField.append(cmdField);
//                                cmdField = new ArrayList<>();
//                            }
//                            cmdField.add(cmd);
//                        }
//                        CmdField.append(cmdField);
//                        return null;
//                    }
//                }
//        ).collect();
    }

    public List<List<String> > getHotKeyValue(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap, HotkeyFinder hotkeyFinder) {
        if (rddMap == null)
            return null;
        if (hotkeyFinder == null)
            return null;
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);
        if (pairRDD.count() == 0L)
            return null;
        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
            List<String> cmdField;
            @Override
            public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                 cmdField = new ArrayList<>();
                 for (Map<String, Object> map: maps) {
                     cmdField.add((String) map.get("i_cmd"));
                 }
                 hotkeyFinder.append(cmdField);
                 return null;
            }
        }).collect();
        return hotkeyFinder.getMaxAppearance(Config.getHotkeyAppearancePercentage());

    }
}
