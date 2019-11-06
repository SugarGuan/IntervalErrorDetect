package core.learn.module;

import core.learn.field.AddrField;
import core.learn.field.CmdField;
import core.learn.field.Frame_typeField;
import core.learn.field.ProtoField;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class HartipModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_hartip");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> frame_typeField;
                    List<String> addrField;
                    List<String> cmdField;
                    List<String> protoField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        frame_typeField = new ArrayList<>();
                        addrField = new ArrayList<>();
                        cmdField = new ArrayList<>();
                        protoField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            frame_typeField.add((String) map.get("i_frame_type"));
                            addrField.add((String) map.get("i_addr"));
                            cmdField.add((String) map.get("i_cmd"));
                            protoField.add((String) map.get("i_proto"));
                        }
                        Frame_typeField.append(frame_typeField);
                        AddrField.append(addrField);
                        CmdField.append(cmdField);
                        ProtoField.append(protoField);
                        return null;
                    }
                }
        ).collect();
    }
}
