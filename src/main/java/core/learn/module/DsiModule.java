package core.learn.module;

import core.learn.field.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class DsiModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_dsi");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

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
                            flagField.add((String) map.get("i_flag"));
                            cmdField.add((String) map.get("i_cmd"));
                            reqidField.add((String) map.get("i_reqid"));
                            offset_errcodeField.add((String) map.get("i_offset_errcode"));
                            datalenField.add((String) map.get("i_datalen"));
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
