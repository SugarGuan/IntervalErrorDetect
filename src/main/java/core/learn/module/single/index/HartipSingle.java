package core.learn.module.single.index;

import core.learn.field.AddrField;
import core.learn.field.CmdField;
import core.learn.field.Frame_typeField;
import core.learn.field.ProtoField;
import core.learn.module.Module;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class HartipSingle extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_hartip");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

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
                            String frame_type = Long.toString((Long) map.get("i_frame_type"));
                            String addr = Long.toString((Long) map.get("i_addr"));
                            String cmd = Long.toString((Long) map.get("i_cmd"));
                            String proto = (String) map.get("i_proto");

                            if (frame_typeField.contains(frame_type)) {
                                Frame_typeField.append(frame_typeField);
                                frame_typeField = new ArrayList<>();
                            }

                            if (addrField.contains(addr)) {
                                AddrField.append(addrField);
                                addrField = new ArrayList<>();
                            }

                            if (cmdField.contains(cmd)) {
                                CmdField.append(cmdField);
                                cmdField = new ArrayList<>();
                            }

                            if (protoField.contains(proto)) {
                                ProtoField.append(protoField);
                                protoField = new ArrayList<>();
                            }

                            frame_typeField.add(frame_type);
                            addrField.add(addr);
                            cmdField.add(cmd);
                            protoField.add(proto);
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
