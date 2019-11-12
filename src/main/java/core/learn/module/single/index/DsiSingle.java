package core.learn.module.single.index;

import core.learn.field.*;
import core.learn.module.Module;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class DsiSingle extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_dsi");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> flagField;
                    List<String> cmdField;
                    List<String> reqidField;
                    List<String> offset_errcodeField;
                    List<String> datalenField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        flagField = new ArrayList<>();
                        cmdField = new ArrayList<>();
                        reqidField = new ArrayList<>();
                        offset_errcodeField = new ArrayList<>();
                        datalenField = new ArrayList<>();

                        for (Map<String, Object> map: maps) {
                            String flag = (String) map.get("i_flag");
                            String cmd = (String) map.get("i_cmd");
                            String reqid = Long.toString((Long) map.get("i_reqid"));
                            String offset_errcode = Long.toString((Long) map.get("i_offset_errcode"));
                            String datalen = Long.toString((Long) map.get("i_datalen"));

                            if (flagField.contains(flag)) {
                                FlagField.append(flagField);
                                flagField = new ArrayList<>();
                            }

                            if (cmdField.contains(cmd)) {
                                CmdField.append(cmdField);
                                cmdField = new ArrayList<>();
                            }

                            if (reqidField.contains(reqid)) {
                                ReqidField.append(reqidField);
                                reqidField = new ArrayList<>();
                            }

                            if (offset_errcodeField.contains(offset_errcode)) {
                                Offset_errcodeField.append(offset_errcodeField);
                                offset_errcodeField = new ArrayList<>();
                            }

                            if (datalenField.contains(datalen)) {
                                DatalenField.append(datalenField);
                                datalenField = new ArrayList<>();
                            }

                            flagField.add(flag);
                            cmdField.add(cmd);
                            reqidField.add(reqid);
                            offset_errcodeField.add(offset_errcode);
                            datalenField.add(datalen);
                        }
                        FlagField.append(flagField);
                        CmdField.append(cmdField);
                        ReqidField.append(reqidField);
                        Offset_errcodeField.append(offset_errcodeField);
                        DatalenField.append(datalenField);
                        return null;
                    }
                }
        ).collect();
    }
}
