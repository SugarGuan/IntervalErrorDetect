package core.learn.module;

import core.learn.field.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class GryphonModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_gryphon");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> frame_typeField;
                    List<String> srcField;
                    List<String> src_cidField;
                    List<String> dstField;
                    List<String> dst_chField;
                    List<String> cmdField;
                    List<String> contextField;
                    List<String> datalenField;

                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        frame_typeField = new ArrayList<>();
                        srcField = new ArrayList<>();
                        src_cidField = new ArrayList<>();
                        dstField = new ArrayList<>();
                        dst_chField = new ArrayList<>();
                        cmdField = new ArrayList<>();
                        contextField = new ArrayList<>();
                        datalenField = new ArrayList<>();

                        for (Map<String, Object> map: maps) {
                            frame_typeField.add((String) map.get("i_frame_type"));
                            srcField.add((String) map.get("i_src"));
                            src_cidField.add((String) map.get("i_src_cid"));
                            dstField.add((String) map.get("i_dst"));
                            dst_chField.add((String) map.get("i_dst_ch"));
                            cmdField.add((String) map.get("i_cmd"));
                            contextField.add((String) map.get("i_context"));
                            datalenField.add((String) map.get("i_datalen"));

                        }

                        Frame_typeField.append(frame_typeField);
                        SrcField.append(srcField);
                        Src_cidField.append(src_cidField);
                        DstField.append(dstField);
                        Dst_chField.append(dst_chField);
                        CmdField.append(cmdField);
                        ContextField.append(contextField);
                        DatalenField.append(datalenField);
                        return null;
                    }
                }
        ).collect();
    }
}
