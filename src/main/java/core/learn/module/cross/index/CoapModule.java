package core.learn.module.cross.index;

import core.learn.field.*;
import core.learn.module.Module;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class CoapModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_coap");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> pkt_typeField;
                    List<String> codeField;
                    List<String> msgidField;
                    List<String> tokenField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        pkt_typeField = new ArrayList<>();
                        codeField = new ArrayList<>();
                        msgidField = new ArrayList<>();
                        tokenField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            pkt_typeField.add((String) map.get("i_pkt_type"));
                            codeField.add((String) map.get("i_code"));
                            if (map.get("i_msgid") instanceof Long)
                                msgidField.add(Long.toString((Long) map.get("i_msgid")));
                            else
                                msgidField.add((String) map.get("i_msgid"));
                            if (map.get("i_token") instanceof Long)
                                msgidField.add(Long.toString((Long) map.get("i_token")));
                            else
                                msgidField.add((String) map.get("i_token"));
                        }
                        Pkt_typeField.append(pkt_typeField);
                        CodeField.append(codeField);
                        MsgidField.append(msgidField);
                        TokenField.append(tokenField);
                        return null;
                    }
                }
        ).collect();
    }
}
