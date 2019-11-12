package core.learn.module.cross.index;

import core.learn.field.DataField;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.*;

public class FoxModule extends Module {

    @Override
    public JavaPairRDD<String, Map<String,Object>> retrieveRDD(Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap){
        return rddMap.get("au_pkt_fox");
    }

    @Override
    public void fieldFillin (Map<String, JavaPairRDD<String, Map<String, Object>>> rddMap) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = this.retrieveRDD(rddMap);

        if (pairRDD == null)
            return ;

        if (pairRDD.count() == 0)
            return ;

        pairRDD.groupByKey().values().map(new Function<Iterable<Map<String, Object>>, Object>() {
                    List<String> dataField;
                    @Override
                    public Object call(Iterable<Map<String, Object>> maps) throws Exception {
                        dataField = new ArrayList<>();
                        for (Map<String, Object> map: maps) {
                            dataField.add((String) map.get("i_data"));
                        }
                        DataField.append(dataField);
                        return null;
                    }
                }
        ).collect();
    }
}
