package core.learn.module;

import core.learn.field.CmdField;
import core.learn.field.ContentField;
import core.learn.field.Pkt_typeField;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class ImapModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_imap");
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
                    List<String> contentField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        pkt_typeField = new ArrayList<>();
                        contentField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            pkt_typeField.add((String) map.get("i_pkt_type"));
                            contentField.add((String) map.get("i_content"));
                        }
                        Pkt_typeField.append(pkt_typeField);
                        ContentField.append(contentField);
                        return null;
                    }
                }
        ).collect();
    }
}
