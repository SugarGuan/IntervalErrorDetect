package core.learn.module.cross.index;

import core.learn.field.*;
import core.learn.module.Module;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import util.StringUtil;

import java.util.*;

public class GryphonModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_gryphon");
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
                            String frame_type =  StringUtil.trans(map.get("i_frame_type"));
                            String src =  StringUtil.trans(map.get("i_src"));
                            String src_cid = StringUtil.trans(map.get("i_src_cid"));
                            String dst =  StringUtil.trans(map.get("i_dst"));
                            String dst_ch = StringUtil.trans(map.get("i_dst_ch"));
                            String cmd =  StringUtil.trans(map.get("i_cmd"));
                            String context = StringUtil.trans(map.get("i_context"));
                            String datalen = StringUtil.trans(map.get("i_datalen"));

                            if (frame_typeField.contains(frame_type)) {
                                Frame_typeField.append(frame_typeField);
                                frame_typeField = new ArrayList<>();
                            }

                            if (srcField.contains(src)) {
                                SrcField.append(srcField);
                                srcField = new ArrayList<>();
                            }

                            if (src_cidField.contains(src_cid)) {
                                Src_cidField.append(src_cidField);
                                src_cidField = new ArrayList<>();
                            }

                            if (dstField.contains(dst)) {
                                DstField.append(dstField);
                                dstField = new ArrayList<>();
                            }

                            if (dst_chField.contains(dst_ch)) {
                                Dst_chField.append(dst_chField);
                                dst_chField = new ArrayList<>();
                            }

                            if (cmdField.contains(cmd)) {
                                CmdField.append(cmdField);
                                cmdField = new ArrayList<>();
                            }

                            if (contextField.contains(context)) {
                                ContextField.append(contextField);
                                contextField = new ArrayList<>();
                            }

                            if (datalenField.contains(datalen)) {
                                DatalenField.append(datalenField);
                                datalenField = new ArrayList<>();
                            }


                            frame_typeField.add(frame_type);
                            srcField.add(src);
                            src_cidField.add(src_cid);
                            dstField.add(dst);
                            dst_chField.add(dst_ch);
                            cmdField.add(cmd);
                            contextField.add(context);
                            datalenField.add(datalen);

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
