package core.learn.module.cross.index;

import core.learn.field.*;
import core.learn.module.Module;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import util.StringUtil;

import java.util.*;

public class EgdModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_egd");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> req_idField;
                    List<String> producer_idField;
                    List<String> exchange_idField;
                    List<String> statusField;
                    List<String> datalenField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        req_idField = new ArrayList<>();
                        producer_idField = new ArrayList<>();
                        exchange_idField = new ArrayList<>();
                        statusField = new ArrayList<>();
                        datalenField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            req_idField.add(StringUtil.trans(map.get("i_req_id")) );
                            producer_idField.add(StringUtil.trans(map.get("i_producer_id")) );
                            exchange_idField.add(StringUtil.trans(map.get("i_exchange_id")) );
                            statusField.add(StringUtil.trans(map.get("i_status")) );
                            datalenField.add(StringUtil.trans(map.get("i_datalen")) );
                        }
                        Req_idField.append(req_idField);
                        Producer_idField.append(producer_idField);
                        Exchange_idField.append(exchange_idField);
                        StatusField.append(statusField);
                        DatalenField.append(datalenField);
                        return null;
                    }
                }
        ).collect();
    }
}
