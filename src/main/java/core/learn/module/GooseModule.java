package core.learn.module;

import core.learn.field.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class GooseModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_goose");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> appidField;
                    List<String> gocbrefField;
                    List<String> datsetField;
                    List<String> goidField;
                    List<String> stnumField;
                    List<String> sqnumField;
                    List<String> testField;
                    List<String> conf_revField;
                    List<String> ndscomField;
                    List<String> entry_numField;
                    List<String> datalenField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {

                        appidField = new ArrayList<>();
                        gocbrefField = new ArrayList<>();
                        datsetField = new ArrayList<>();
                        goidField = new ArrayList<>();
                        stnumField = new ArrayList<>();
                        sqnumField = new ArrayList<>();
                        testField = new ArrayList<>();
                        conf_revField = new ArrayList<>();
                        ndscomField = new ArrayList<>();
                        entry_numField = new ArrayList<>();
                        datalenField = new ArrayList<>();

                        for (Map<String, Object> map: maps) {
                            appidField.add((String) map.get("i_appid"));
                            gocbrefField.add((String) map.get("i_gocbref"));
                            datsetField.add((String) map.get("i_datset"));
                            goidField.add((String) map.get("i_goid"));
                            stnumField.add((String) map.get("i_stnum"));
                            sqnumField.add((String) map.get("i_sqnum"));
                            testField.add((String) map.get("i_test"));
                            conf_revField.add((String) map.get("i_conf_rev"));
                            ndscomField.add((String) map.get("i_ndscom"));
                            entry_numField.add((String) map.get("i_entry_num"));
                            datalenField.add((String) map.get("i_datalen"));
                        }

                        AppidField.append(appidField);
                        GocbrefField.append(gocbrefField);
                        DatsetField.append(datsetField);
                        GoidField.append(goidField);
                        StnumField.append(stnumField);
                        SqnumField.append(sqnumField);
                        TestField.append(testField);
                        Conf_revField.append(conf_revField);
                        NdscomField.append(ndscomField);
                        Entry_numField.append(entry_numField);
                        DatalenField.append(datalenField);
                        return null;
                    }
                }
        ).collect();
    }
}
