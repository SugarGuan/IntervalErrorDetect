package core.learn.module.cross.index;

import core.learn.HotkeyFinder;
import core.learn.field.CmdField;
import core.learn.module.Module;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import util.StringUtil;

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

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> cmdField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        cmdField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            String cmd = StringUtil.trans(map.get("i_cmd"));
                            cmdField.add(cmd);
                        }
                        CmdField.append(cmdField);
                        return null;
                    }
                }
        ).collect();
    }

    public Map<String, List<List<String>>> learn() {
        Map<String, List<List<String>>> result = new HashMap<>();
        HotkeyFinder f = new HotkeyFinder();
        f.appendOperationLists(CmdField.getStrList());
        result.put("cmd", f.getFrequentOperationList());
        return result;
    }
}
