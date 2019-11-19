package core.learn.module.cross.index;

import core.learn.field.*;
import core.learn.module.Module;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import util.StringUtil;

import java.util.*;

public class PowerlinkModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_powerlink");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> layerField;
                    List<String> msgtypeField;
                    List<String> msgtype_strField;
                    List<String> destField;
                    List<String> dest_strField;
                    List<String> sourceField;
                    List<String> source_strField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        layerField = new ArrayList<>();
                        msgtypeField = new ArrayList<>();
                        msgtype_strField = new ArrayList<>();
                        destField = new ArrayList<>();
                        dest_strField = new ArrayList<>();
                        sourceField = new ArrayList<>();
                        source_strField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            layerField.add(StringUtil.trans(map.get("i_layer")));
                            msgtypeField.add(StringUtil.trans(map.get("i_msgtype")));
                            msgtype_strField.add(StringUtil.trans(map.get("i_msgtype_str")));
                            destField.add(StringUtil.trans(map.get("i_dest")));
                            dest_strField.add(StringUtil.trans(map.get("i_dest_str")));
                            sourceField.add(StringUtil.trans(map.get("i_source")));
                            source_strField.add(StringUtil.trans(map.get("i_source_str")));
                        }
                        LayerField.append(layerField);
                        MsgtypeField.append(msgtypeField);
                        Msgtype_strField.append(msgtype_strField);
                        DestField.append(destField);
                        Dest_strField.append(dest_strField);
                        SourceField.append(sourceField);
                        SourceField.append(source_strField);
                        return null;
                    }
                }
        ).collect();
    }
}
